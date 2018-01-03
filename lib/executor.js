
var mod_crypto = require('crypto');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');
var mod_uuid = require('uuid');
var mod_moray = require('moray');

var VE = mod_verror.VError;

var lib_triton_access = require('./triton_access');
var lib_data_access = require('./data_access');
var lib_serial = require('./serial');
var lib_status = require('./status');
var lib_locks = require('./locks');
var lib_create_instance = require('./create_instance');


var PHASES = [
	'check_postgres_version',
	'freeze_cluster',

	/*
	 * XXX New phase:
	 *	- create the global PG dump interlock, and kill off any
	 *	  running dumps in any PG shard in the system
	 *
	 * NOTE that this will require some kind of reference counting
	 * approach, so that dumps remain disabled until _no_ plans
	 * require them to be disabled.
	 */

	'create_sentinel_bucket',
	'create_manatee_peers',

	/*
	 * XXX At this point in the plan, we need to serialise execution so
	 * that two plans don't stomp on one another (generating new hash
	 * rings, confirming that two plans do not create the same new shard,
	 * etc).  This serialisation needs to persist in the database so that
	 * it cannot be broken even if we restart unexpectedly.
	 */
	'critical_section_enter',

	'remap_ring',

	'update_sapi',
	'restart_electric_moray',

	'shutdown_new_peers',
	'reconfigure_peers',
	'establish_cluster',
	'create_morays',
	'install_ring',

	/*
	 * XXX New phase:
	 *	- deploy new hash ring database into every Electric Moray
	 *	  instance (disable registrar, wait, reprovision same image?)
	 */
	'update_electric_moray_ring',

	'update_sapi_again',
	'restart_electric_moray_again',
	'sapi_cleanup',

	/*
	 * XXX Here we can return to parallel execution, allowing another
	 * plan to proceed into the critical region.
	 */
	'critical_section_exit',

	/*
	 * XXX New phase:
	 *	- unfreeze manatee shards
	 */

	/*
	 * XXX New phase:
	 *	- begin deleting the ghost half of the data on each side
	 *	  of the split (i.e., the half of vnodes from the target
	 *	  shard which were moved, and the half of vnodes from the
	 *	  new shard which were not)
	 */
	'delete_data',

	/*
	 * XXX New phase:
	 *	- release our hold on the global PG dump interlock
	 */

	'holding_pattern'
];

function
PlanRun(exe, plan)
{
	var self = this;

	mod_assert.object(exe, 'exe');

	mod_assert.object(plan, 'plan');
	mod_assert.bool(plan.active, 'plan.active');
	mod_assert.uuid(plan.uuid, 'plan.uuid');
	mod_assert.string(plan._etag, 'plan._etag');
	mod_assert.optionalString(plan.phase, 'plan.phase');

	mod_assert.ok(plan.active, 'plan must be active to be run'); /* XXX */

	self.pr_exe = exe;
	self.pr_plan = mod_jsprim.deepCopy(plan);

	/*
	 * At times we will need to make requests to the target shard
	 * via Moray.  We create a standing Moray client that we can pass
	 * to phases that require it.
	 */
	var domain_name = exe.exe_ctx.ctx_app.metadata.DOMAIN_NAME;
	mod_assert.string(domain_name, 'DOMAIN_NAME');

	var moray_cfg = function (shard) {
		mod_assert.string(shard, 'shard');

		return ({
			srvDomain: shard,
			cueballOptions: {
				resolvers: [
					'nameservice.' + domain_name
				]
			},
			log: exe.exe_log.child({ plan: plan.uuid, moray: true })
		});
	};
	self.pr_moray_cfg = moray_cfg(plan.shard);
	self.pr_new_moray_cfg = moray_cfg(plan.new_shard);

	/*
	 * XXX this needs to be disconnected when the plan run completes.
	 */
	self.pr_moray = null;
	self.pr_new_moray = null;

	self.pr_running = false;
	self.pr_commit = false;

	self.pr_status = new lib_status.Status();
	self.pr_status.update('plan "%s"', self.pr_plan.uuid);

	self.pr_http_handlers = {};

	self.pr_retry_info = null;
}

/*
 * This function is called to drive execution of the phase function for this
 * plan run.  It is called whenever control flow comes to rest; e.g., after the
 * callback for a particular phase is called.  Calling the function when
 * execution is already in progress is safe, but has no effect.
 */
PlanRun.prototype.dispatch = function
dispatch()
{
	var self = this;

	var log = self.pr_exe.exe_log;

	mod_assert.bool(self.pr_running, 'pr_running');
	if (self.pr_running) {
		return;
	}
	self.pr_running = true;

	/*
	 * Clear out any latent child status notes.
	 */
	self.pr_status.trunc();

	if (self.pr_plan.hold !== null) {
		self.pr_status.update('plan "%s": on hold, waiting for ' +
		    'operator', self.pr_plan.uuid);
		self.pr_running = false;
		log.info('plan "%s" is in the hold state; ignoring',
		    self.pr_plan.uuid);
		return;
	}

	mod_assert.bool(self.pr_plan.paused, 'paused');
	if (self.pr_plan.paused) {
		self.pr_status.update('plan "%s": paused', self.pr_plan.uuid);
		self.pr_running = false;
		log.info('plan "%s" marked paused; ignoring',
		    self.pr_plan.uuid);
		return;
	}

	mod_assert.bool(self.pr_plan.completed, 'completed');
	if (self.pr_plan.completed) {
		self.pr_status.update('plan "%s": complete', self.pr_plan.uuid);
		log.info('plan "%s" completed already; nothing to do',
		    self.pr_plan.uuid);
		return;
	}

	if (self.pr_moray === null) {
		self.pr_status.update('plan "%s": connecting Moray (shard %s)',
		    self.pr_plan.uuid, self.pr_plan.shard);
		log.info('connecting standing target shard Moray');
		var mc = mod_moray.createClient(self.pr_moray_cfg);

		mc.once('connect', function () {
			log.info('standing target shard Moray connected');

			self.pr_moray = mc;

			self.pr_running = false;
			self.dispatch();
		});
		return;
	}

	if (self.pr_plan.phase === null) {
		/*
		 * This plan has not yet started to run.  Begin with the first
		 * phase in the list.
		 */
		self.pr_plan.phase = PHASES[0];
		log.info('plan starting with phase "%s"', self.pr_plan.phase);

	} else {
		/*
		 * This plan has been running already.  Make sure it is up
		 * to a phase which still exists.
		 */
		if (PHASES.indexOf(self.pr_plan.phase) === -1) {
			/*
			 * This phase does not exist in the current software
			 * version.  Stop execution of this plan with an error
			 * we can report to the operator.
			 */
			self.hold(new VE('phase "%s" is invalid',
			    self.pr_plan.phase));
			return;
		}
	}

	var exe = self.pr_exe;
	var ctx = self.pr_exe.exe_ctx;
	var cfg = ctx.ctx_cfg;

	self.pr_status.update('plan "%s": executing phase "%s"',
	    self.pr_plan.uuid, self.pr_plan.phase);
	log.info('plan "%s" running in phase "%s"', self.pr_plan.uuid,
	    self.pr_plan.phase);

	/*
	 * Clear out any HTTP handler functions registered in previous
	 * executions.
	 */
	self.pr_http_handlers = {};

	var ctl_called_back = false;
	var assert_one_call = function () {
		mod_assert.ok(!ctl_called_back, 'called back twice');
		ctl_called_back = true;
	};
	var ctl = {
		make_alias: function (uuid, service) {
			mod_assert.uuid(uuid, 'uuid');
			mod_assert.string(service, 'service');

			return (ctl.new_short_shard() + '.' + service + '.' +
			    cfg.region + '.' + cfg.dns_domain + '-' +
			    uuid.substr(0, 8));
		},
		new_short_shard: function () {
			var tail = cfg.region + '.' + cfg.dns_domain;
			var re = new RegExp('(.+)\\.moray\\.' +
			    tail.replace(/\./g, '\\.'));

			var m = re.exec(self.pr_plan.new_shard);
			mod_assert.string(m[1], 'm[1]');

			return (m[1]);
		},
		short_shard: function () {
			var tail = cfg.region + '.' + cfg.dns_domain;
			var re = new RegExp('(.+)\\.moray\\.' +
			    tail.replace(/\./g, '\\.'));

			var m = re.exec(self.pr_plan.shard);
			mod_assert.string(m[1], 'm[1]');

			return (m[1]);
		},
		plan: function () {
			return (mod_jsprim.deepCopy(self.pr_plan));
		},
		log: log.child({
			plan: self.pr_plan.uuid,
			phase: self.pr_plan.phase
		}),
		status: function () {
			return (self.pr_status.child());
		},
		pausing: function (callback) {
			mod_assert.func(callback, 'callback');
			mod_assert.bool(self.pr_plan.paused, 'paused');

			if (!self.pr_plan.paused) {
				return (false);
			}

			/*
			 * Pass this sentinel in the error we create so
			 * that we can detect it in the hold()/retry()
			 * routines as induced by pausing a plan.
			 */
			var i = { info: { exe_pause: true }};

			setImmediate(callback, new VE(i, 'pausing'));
			return (true);
		},

		prop_put: function (name, val) {
			mod_assert.ok(typeof (val) === 'string' ||
			    typeof (val) === 'number', 'invalid prop type');

			self.pr_plan.props[name] = val;
		},
		prop_get: function (name) {
			if (self.pr_plan.props.hasOwnProperty(name)) {
				return (self.pr_plan.props[name]);
			}

			return (null);
		},
		prop_del: function (name) {
			delete (self.pr_plan.props[name]);
		},
		prop_commit: function (callback) {
			/*
			 * XXX
			 */
			self.commit(callback);
		},

		get_image: function (uuid, callback) {
			lib_triton_access.get_hashring_image(ctx, uuid,
			    callback);
		},
		list_images: function (opts, callback) {
			lib_triton_access.list_hashring_images(ctx, opts,
			    callback);
		},
		get_manta_app: function (callback) {
			lib_triton_access.refresh_manta_application(ctx,
			    callback);
		},
		get_instance: function (uuid, callback) {
			mod_assert.uuid(uuid, 'uuid');
			mod_assert.func(callback, 'callback');

			lib_triton_access.get_instance(ctx, uuid, callback);
		},
		get_instances: function (filter, callback) {
			mod_assert.object(filter, 'filter');
			mod_assert.func(callback, 'callback');

			lib_triton_access.get_instances(ctx, filter, callback);
		},
		get_instance_ip: function (uuid, callback) {
			lib_triton_access.get_instance_ip(ctx, uuid, callback);
		},
		create_instance: function (opts, callback) {
			lib_triton_access.get_service(ctx, opts.service,
			    function (err, res) {
				if (err) {
					callback(err);
					return;
				}

				lib_create_instance.create_instance(ctx, {
					service: res,
					shard: opts.shard,
					server_uuid: opts.server_uuid,
					image_uuid: opts.image_uuid,
					datacenter_name:
					    opts.datacenter_name,
					tags: opts.tags,
				}, callback);
			});
		},
		update_app: function (uuid, opts, callback) {
			mod_assert.uuid(uuid, 'uuid');
			mod_assert.object(opts, 'opts');
			mod_assert.func(callback, 'callback');

			var reqopts = {
				action: 'update',
				master: true,
				metadata: opts.metadata,
				params: opts.params
			};

			ctx.ctx_sapi.updateApplication(uuid, reqopts, callback);
		},
		update_instance: function (uuid, opts, callback) {
			mod_assert.uuid(uuid, 'uuid');
			mod_assert.object(opts, 'opts');
			mod_assert.func(callback, 'callback');

			var reqopts = {
				action: 'update',
				master: true,
				metadata: opts.metadata,
				params: opts.params
			};

			ctx.ctx_sapi.updateInstance(uuid, reqopts, callback);
		},
		reprovision_same_image: function (uuid, callback) {
			mod_assert.uuid(uuid, 'uuid');
			mod_assert.func(callback, 'callback');

			ctx.ctx_sapi.getInstance(uuid, function (err, inst) {
				if (err) {
					callback(err);
					return;
				}

				var sapi = ctx.ctx_dcs[
				    inst.metadata.DATACENTER].dc_clients.
				    dcc_sapi;

				ctx.ctx_log.info('reprovisioning instance ' +
				    '"%s" in DC "%s" to image "%s"',
				    uuid, inst.metadata.DATACENTER,
				    inst.params.image_uuid);

				sapi.reprovisionInstance(uuid,
				    inst.params.image_uuid, function (err) {
					if (!err) {
						callback();
						return;
					}

					if (!err.message.match(
					    /job reprovision/)) {
						callback(err);
						return;
					}

					/*
					 * The way reprovision errors are
					 * reported by the SAPI client is
					 * deeply regrettable.  It seems
					 * as if an entire log file is jammed
					 * into the "message" property.
					 *
					 * Try to clean up the mess.
					 */
					var info = { hold: true };
					var lines = err.message.trim().
					    split('\n');

					var msg = lines[0].replace(
					    /: \{.*/, '');

					if (lines.length > 1) {
						msg += '; ' +
						    lines[lines.length - 1];
					}

					callback(new VE({ info: info }, '%s',
					    msg));
				});
			});
		},
		set_vm_alias: function (uuid, alias, callback) {
			lib_triton_access.update_vmapi_vm(ctx, uuid,
			    { alias: alias }, function (err, obj, req, res) {
				var body;
				if (err) {
					try {
						body = JSON.parse(res.body);
					} catch (ex) {
					}
					ctx.ctx_log.warn({ err: err,
					    body: body }, 'VMAPI error');
				}
				if (body && body.errors &&
				    body.errors[0].field === 'alias' &&
				    body.errors[0].code === 'Duplicate') {
					ctx.ctx_log.info('assuming duplicate ' +
					    'alias error is idempotency ' +
					    'issue');
					callback();
					return;
				}
				callback(err);
			});
		},
		zone_exec: function (zone_uuid, script, callback) {
			lib_triton_access.zone_exec(ctx, zone_uuid,
			    script, callback);
		},
		new_shard_moray: function (callback) {
			/*
			 * XXX We need to avoid creating two clients
			 * if we are called twice before the first client
			 * connects.
			 */
			if (self.pr_new_moray !== null) {
				setImmediate(callback, null, self.pr_new_moray);
				return;
			}

			log.info('connecting standing new shard Moray');
			var mc = mod_moray.createClient(self.pr_new_moray_cfg);

			var ignore = false;
			var check_pause = setInterval(function () {
				if (!ctl.pausing(callback)) {
					return;
				}

				ignore = true;
				clearInterval(check_pause);
				mc.close();
			}, 1000);

			mc.once('connect', function () {
				if (ignore) {
					return;
				}
				clearInterval(check_pause);
				log.info('standing new shard Moray connected');

				self.pr_new_moray = mc;
				setImmediate(callback, null, self.pr_new_moray);
			});
		},
		target_moray: function () {
			mod_assert.object(self.pr_moray, 'pr_moray');

			return (self.pr_moray);
		},
		register_http_handler: function (handler_func) {
			mod_assert.func(handler_func, 'handler_func');

			var token;
			for (;;) {
				token = mod_crypto.randomBytes(4).toString(
				    'hex');

				if (!self.pr_http_handlers[token]) {
					break;
				}
			}

			self.pr_http_handlers[token] = handler_func;

			return ('http://' + cfg.manta_ip + '/update/' +
			    self.pr_plan.uuid + '/' + token);
		},

		lock: function (name, callback) {
			/*
			 * XXX We should probably be recording the locks that
			 * a plan has taken, and re-verifying them if the
			 * process restarts.
			 */
			exe.exe_locks.lock(name, 'plan:' + self.pr_plan.uuid,
			    callback);
		},
		unlock: function (name, callback) {
			exe.exe_locks.unlock(name, 'plan:' + self.pr_plan.uuid,
			    callback);
		},

		/*
		 * Finishing functions.  At most one of "retry", "hold", or
		 * "finish" should be called, and the selected function must be
		 * called at most once.
		 */
		retry: function (err) {
			assert_one_call();

			self.pr_status.trunc();

			log.warn(err, 'plan "%s" phase "%s" failed (retrying)',
			    self.pr_plan.uuid, self.pr_plan.phase);

			self.retry(err);
		},
		hold: function (err) {
			assert_one_call();

			self.pr_status.trunc();

			log.warn(err, 'plan "%s" phase "%s" holding execution',
			    self.pr_plan.uuid, self.pr_plan.phase);

			self.hold(err);
		},
		finish: function () {
			assert_one_call();

			self.pr_status.trunc();

			var idx = PHASES.indexOf(self.pr_plan.phase);
			if (idx + 1 < PHASES.length) {
				/*
				 * Move to next phase.
				 */
				self.pr_plan.phase = PHASES[idx + 1];
				log.info('phase completed');
			} else {
				/*
				 * Plan execution complete!
				 */
				self.pr_plan.completed = true;
				log.info('plan completed');
			}

			setImmediate(function () {
				self.commit(function () {
					self.pr_running = false;
					self.dispatch();
				});
			});
		},
	};

	var phase_func = require('./phase_' + self.pr_plan.phase + '.js')[
	    'phase_' + self.pr_plan.phase];

	phase_func(ctl);
};

PlanRun.prototype.retry = function
retry(err)
{
	var self = this;
	var log = self.pr_exe.exe_log;

	mod_assert.ok(err instanceof Error, 'err must be an Error');
	mod_assert.ok(self.pr_running, 'must be running');

	if (VE.info(err).hold || VE.info(err).exe_pause) {
		log.warn('retry called with a holding error');
		self.hold(err);
		return;
	}

	mod_assert.ok(!self.pr_retry_info, 'should not have existing retry');
	self.pr_retry_info = {
		name: err.name,
		code: err.code,
		message: err.message,
		info: VE.info(err),
		stack: VE.fullStack(err)
	};

	self.pr_status.update('plan "%s": waiting to retry ' +
	    'phase "%s"', self.pr_plan.uuid,
	    self.pr_plan.phase);

	setTimeout(function () {
		self.pr_retry_info = null;
		self.pr_running = false;
		self.dispatch();
	}, 10 * 1000);
};

PlanRun.prototype.hold = function
hold(err)
{
	var self = this;
	var log = self.pr_exe.exe_log;

	mod_assert.ok(err instanceof Error, 'err must be an Error');
	mod_assert.ok(self.pr_running, 'must be running');

	if (VE.info(err).exe_pause) {
		log.info(err, 'plan paused');
		self.commit(function () {
			self.pr_running = false;
			self.dispatch();
		});
		return;
	}

	mod_assert.ok(!self.pr_plan.hold, 'should not have existing hold');
	self.pr_plan.hold = {
		name: err.name,
		code: err.code,
		message: err.message,
		info: VE.info(err),
		stack: VE.fullStack(err)
	};

	self.commit(function () {
		self.pr_running = false;
		self.dispatch();
	});
};

PlanRun.prototype.commit = function
commit(callback)
{
	var self = this;

	mod_assert.ok(!self.pr_commit, 'commit in progress already');
	self.pr_commit = true;

	var log = self.pr_exe.exe_log;
	var old_etag = self.pr_plan._etag;
	var baton;

	mod_vasync.waterfall([ function (done) {
		self.pr_exe.exe_q_moray.run(function (err, _baton) {
			if (err) {
				done(err);
				return;
			}

			baton = _baton;
			done();
		});

	}, function (done) {
		lib_data_access.plan_store(self.pr_exe.exe_ctx, self.pr_plan,
		    done);

	}, function (newplan, done) {
		log.info('plan "%s" committed (etag %s -> %s)',
		    self.pr_plan.uuid, old_etag, newplan._etag);
		self.pr_plan = newplan;

		setImmediate(done);

	} ], function (err) {
		if (baton) {
			baton.release();
		}

		if (err) {
			log.warn(err, 'could not commit plan "%s" (retrying)',
			    self.pr_plan.uuid);
			self.pr_commit = false;
			setTimeout(function () {
				self.commit(callback);
			}, 5000);
			return;
		}

		self.pr_commit = false;
		callback();
	});
};


function
Executor(opts)
{
	var self = this;

	mod_assert.object(opts, 'opts');
	mod_assert.object(opts.ctx, 'opts.ctx');
	mod_assert.object(opts.log, 'opts.log');

	self.exe_ctx = opts.ctx;
	self.exe_log = opts.log;

	self.exe_locks = new lib_locks.Locks(opts.ctx);

	self.exe_q_create = new lib_serial.SerialQueue('create_plan');

	/*
	 * XXX describe why this is needed (to prevent races between _swtch()
	 * and commit()).
	 */
	self.exe_q_moray = new lib_serial.SerialQueue('moray');

	self.exe_plan_run = {};
	self.exe_switching = false;

	setImmediate(function () {
		self._swtch();
	});
}

Executor.prototype._start_plan = function
_start_plan(plan)
{
	var self = this;
	
	mod_assert.object(plan, 'plan');
	mod_assert.uuid(plan.uuid, 'plan.uuid');
	mod_assert.ok(!self.exe_plan_run[plan.uuid], 'plan started already');

	var pr = self.exe_plan_run[plan.uuid] = new PlanRun(self, plan);

	setImmediate(function () {
		self.exe_log.info('starting execution of plan "%s"', plan.uuid);
		pr.dispatch();
	});
};

Executor.prototype._swtch = function
_swtch()
{
	var self = this;

	if (self.exe_switching) {
		return;
	}
	self.exe_switching = true;

	var baton;
	mod_vasync.waterfall([ function (done) {
		self.exe_q_moray.run(function (err, _baton) {
			if (err) {
				done(err);
				return;
			}

			baton = _baton;
			done();
		});

	}, function (done) {
		/*
		 * Load plans from database.
		 */
		lib_data_access.plans_active(self.exe_ctx,
		    function (err, plans) {
			if (err) {
				self.exe_log.error(err, 'loading active plans');
				done(err);
				return;
			}

			done(null, plans);
		});

	}, function (plans, done) {
		plans.forEach(function (plan) {
			mod_assert.uuid(plan.uuid, 'plan.uuid');

			if (!self.exe_plan_run[plan.uuid]) {
				/*
				 * This plan is not currently executing.
				 * Start plan execution.
				 */
				self._start_plan(plan);
				return;
			}

			mod_assert.equal(plan._etag,
			    self.exe_plan_run[plan.uuid].pr_plan._etag,
			    'etag for ' + plan.uuid + ' changed while we ' +
			    'were not looking?!');
		});

		setImmediate(done);

	} ], function (err) {
		if (err) {
			self.exe_log.error(err, '_swtch');
		}

		if (baton) {
			baton.release();
		}

		self.exe_switching = false;
		setTimeout(function () {
			self._swtch();
		}, 5000);
	});
};

/*
 * Called by HTTP server when a request is made to resume a paused plan.
 */
Executor.prototype.handle_resume = function
handle_resume(opts, callback)
{
	var self = this;

	mod_assert.uuid(opts.plan_uuid, 'plan_uuid');
	mod_assert.func(callback, 'callback');

	var pr = self.exe_plan_run[opts.plan_uuid];
	if (!pr) {
		callback(new VE('plan "%s" is not running', opts.plan_uuid));
		return;
	}

	/*
	 * Mark the plan as resumed and kick the dispatcher.
	 * XXX We should wait until this is at least persisted in the database.
	 */
	pr.pr_plan.paused = false;
	setImmediate(function () {
		pr.dispatch();
	});

	setImmediate(callback);
};

/*
 * Called by HTTP server when a request is made to pause a plan.
 */
Executor.prototype.handle_pause = function
handle_pause(opts, callback)
{
	var self = this;

	mod_assert.uuid(opts.plan_uuid, 'plan_uuid');
	mod_assert.func(callback, 'callback');

	var pr = self.exe_plan_run[opts.plan_uuid];
	if (!pr) {
		callback(new VE('plan "%s" is not running', opts.plan_uuid));
		return;
	}

	/*
	 * Mark the plan as pausing, so that it can be brought to an orderly
	 * stop.
	 * XXX We should wait until this is at least persisted in the database.
	 */
	pr.pr_plan.paused = true;

	setImmediate(callback);
};

/*
 * Called by the HTTP server when we receive a status update from a remote
 * component.
 */
Executor.prototype.handle_update = function
handle_update(opts, callback)
{
	var self = this;

	mod_assert.uuid(opts.plan_uuid, 'plan_uuid');
	mod_assert.string(opts.token, 'token');

	mod_assert.object(opts.body, 'body');

	var pr = self.exe_plan_run[opts.plan_uuid];
	if (!pr) {
		callback(new VE('plan "%s" is not running', opts.plan_uuid));
		return;
	}

	var handler = pr.pr_http_handlers[opts.token];
	if (!handler) {
		callback(new VE('plan "%s" has not registered handler ' +
		    '"%s"', opts.plan_uuid, opts.token));
		return;
	}

	handler(opts.body, callback);
};

/*
 * A plan seeks to take an existing shard and split it into some number of
 * new shards.
 *
 *	Inputs:
 *		"shard" (e.g., "1.moray")
 *		"split_count" (e.g., 2 would make "1.moray" into "1.moray" and,
 *		                   say, "36.moray")
 *		"server_list" (e.g., UUIDs of 3 servers per new shard)
 */
Executor.prototype.create_plan = function
create_plan(plan_opts, callback)
{
	var self = this;
	var ctx = self.exe_ctx;
	var log = self.exe_log;

	mod_assert.object(ctx, 'ctx');
	mod_assert.object(ctx.ctx_moray, 'ctx.ctx_moray');

	var plan = {};
	var baton;

	/*
	 * Perform basic validation of the plan options before we make any
	 * requests.
	 * XXX Use a schema validator?
	 * XXX Do this in the HTTP server and just assert here?
	 */
	if (typeof (plan_opts.shard) !== 'string') {
		callback(new VE('require "shard"'));
		return;
	}
	if (typeof (plan_opts.new_shard) !== 'string') {
		callback(new VE('require "new_shard"'));
		return;
	}
	if (typeof (plan_opts.split_count) !== 'number' ||
	    plan_opts.split_count < 2 ||
	    plan_opts.split_count > 2) {
		/*
		 * XXX relax the range?
		 */
		callback(new VE('require "split_count", a number = 2'));
		return;
	}
	if (!Array.isArray(plan_opts.server_list)) {
		callback(new VE('require "server_list", an array of server ' +
		    'UUIDs'));
		return;
	}

	plan.split_count = plan_opts.split_count;

	mod_vasync.waterfall([ function (next) {
		/*
		 * Serialise plan creation as we need to ensure that two
		 * plans do not exist to concurrently modify the same shard.
		 */
		self.exe_q_create.run(function (err, _baton) {
			if (err) {
				next(err);
				return;
			}

			baton = _baton;
			next();
		});

	}, function (next) {
		/*
		 * Determine whether this is a shard in the "index" role.
		 */
		lib_triton_access.refresh_manta_application(ctx,
		    function (err, app) {
			if (err) {
				next(err);
				return;
			}

			if (!app || !app.metadata ||
			    !app.metadata.INDEX_MORAY_SHARDS ||
			    !Array.isArray(app.metadata.INDEX_MORAY_SHARDS)) {
				next(new VE('INDEX_MORAY_SHARDS metadata ' +
				    'invalid'));
				return;
			}

			var candidates = app.metadata.INDEX_MORAY_SHARDS.map(
			    function (ent) {
				mod_assert.string(ent.host, 'ent.host');

				return (ent.host);
			});

			/*
			 * XXX What if this shard has _both_ the Index _and_ a
			 * Storage/Marlin role?
			 */
			if (candidates.indexOf(plan_opts.shard) === -1) {
				var i = { candidates: candidates };
				next(new VE({ info: i }, 'shard "%s" not found',
				    plan_opts.shard));
				return;
			}

			/*
			 * XXX Check also if this shard is in the two unsharded
			 * roles.
			 */
			if (candidates.indexOf(plan_opts.new_shard) !== -1) {
				next(new VE('shard "%s" exists already',
				    plan_opts.new_shard));
				return;
			}

			plan.shard = plan_opts.shard;
			plan.new_shard = plan_opts.new_shard;
			setImmediate(next);
		});

	}, function (next) {
		/*
		 * Check that the list of servers is valid.
		 */
		plan.servers = [];
		mod_vasync.forEachPipeline({ inputs: plan_opts.server_list,
		    func: function (uuid, done) {
			lib_triton_access.find_server(ctx, uuid,
			    function (err, server) {
				if (err) {
					done(err);
					return;
				}

				plan.servers.push(server);
				done();
			});
		}}, function (err) {
			if (err) {
				next(err);
				return;
			}

			next();
		});
	}, function (next) {
		/*
		 * Make sure there are no other active plans for this shard.
		 */
		lib_data_access.plans_for_shard(ctx, plan.shard,
		    function (err, plans) {
			if (err) {
				next(err);
				return;
			}

			if (plans.length !== 0) {
				var i = { conflicting_plans: plans };

				next(new VE({ info: i }, 'conflict with ' +
				    'existing plans'));
				return;
			}

			next();
		});

	}, function (next) {
		/*
		 * Give the plan a UUID and store to Moray.
		 */
		plan.uuid = mod_uuid.v4();
		plan._etag = null;
		plan.active = true;
		plan.completed = false;
		plan.paused = false;
		plan.phase = null;
		plan.props = {};

		lib_data_access.plan_store(ctx, plan, function (err, newplan) {
			if (err) {
				next(err);
				return;
			}

			log.info({ plan: newplan }, 'created plan');

			plan = newplan;
			next();
		});

	} ], function (err) {
		if (baton) {
			baton.release();
		}

		if (err) {
			callback(new VE(err, 'plan error'));
			return;
		}

		callback(null, plan);
	});
};


function
create_executor(ctx, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.func(callback, 'callback');

	ctx.ctx_exec = new Executor({
		ctx: ctx,
		log: ctx.ctx_log.child({ component: 'executor' })
	});

	setImmediate(callback);
}


module.exports = {
	create_executor: create_executor,
};
