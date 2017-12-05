
var mod_path = require('path');
var mod_fs = require('fs');
var mod_crypto = require('crypto');

var mod_assert = require('assert-plus');
var mod_restify = require('restify');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');
var mod_uuid = require('uuid');
var mod_moray = require('moray');

var VE = mod_verror.VError;

var lib_triton_access = require('./triton_access');
var lib_data_access = require('./data_access');
var lib_serial = require('./serial');
var lib_create_instance = require('./create_instance');


var PHASES = [
	'check_postgres_version',
	'freeze_cluster',
	'create_sentinel_bucket',
	'create_manatee_peers',
	'create_read_only_ring',
	'holding_pattern'
];

function
calc_md5(plan_uuid, name, callback)
{
	mod_assert.uuid(plan_uuid, 'plan_uuid');
	mod_assert.string(name, 'name');
	mod_assert.func(callback, 'callback');

	var hash = mod_crypto.createHash('md5');

	/*
	 * XXX
	 */
	var file = mod_path.join('/var/tmp/reshard_files', plan_uuid,
	    name);
	var instr = mod_fs.createReadStream(file);

	instr.on('readable', function () {
		var d;

		while ((d = instr.read()) !== null) {
			hash.update(d);
		}
	});

	instr.on('error', function (err) {
		callback(new VE(err, 'open file "%s"', file));
	});

	instr.on('end', function () {
		var sum = hash.digest('hex');
		callback(null, sum);
	});
}


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
	self.pr_moray_cfg = {
		srvDomain: plan.shard,
		cueballOptions: {
			resolvers: [
				'nameservice.' + domain_name
			]
		},
		log: exe.exe_log.child({ plan: plan.uuid, moray: true })
	};
	/*
	 * XXX this needs to be disconnected when the plan run completes.
	 */
	self.pr_moray = null;

	self.pr_running = false;
	self.pr_commit = false;
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

	if (self.pr_moray === null) {
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

	if (self.pr_plan.hold !== null) {
		self.pr_running = false;
		log.info('plan "%s" is in the hold state; ignoring',
		    self.pr_plan.uuid);
		return;
	}

	mod_assert.bool(self.pr_plan.completed, 'completed');
	if (self.pr_plan.completed) {
		log.info('plan "%s" completed already; nothing to do',
		    self.pr_plan.uuid);
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

	var ctx = self.pr_exe.exe_ctx;
	var cfg = ctx.ctx_cfg;

	log.info('plan "%s" running in phase "%s"', self.pr_plan.uuid,
	    self.pr_plan.phase);

	var ctl_called_back = false;
	var assert_one_call = function () {
		mod_assert.ok(!ctl_called_back, 'called back twice');
	};
	var ctl = {
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

		put_url: function (name) {
			mod_assert.string(name, 'name');

			return ('http://' + cfg.manta_ip + '/file/' +
			    self.pr_plan.uuid + '/' + name);
		},
		verify_file: function (name, md5, callback) {
			calc_md5(self.pr_plan.uuid, name, function (err, chk) {
				if (err) {
					callback(err);
					return;
				}

				if (chk !== md5) {
					callback(new VE('real hash "%s" did ' +
					    'not match expected hash "%s"',
					    chk, md5));
					return;
				}

				callback();
			});
		},

		get_manta_app: function (callback) {
			lib_triton_access.refresh_manta_application(ctx,
			    callback);
		},
		get_instances: function (filter, callback) {
			mod_assert.object(filter, 'filter');
			mod_assert.func(callback, 'callback');

			lib_triton_access.get_instances(ctx, filter, callback);
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
		zone_exec: function (zone_uuid, script, callback) {
			lib_triton_access.zone_exec(ctx, zone_uuid,
			    script, callback);
		},
		target_moray: function () {
			mod_assert.object(self.pr_moray, 'pr_moray');

			return (self.pr_moray);
		},
		moray_client: function (shard, callback) {
			mod_assert.string(shard, 'shard');
			mod_assert.func(callback, 'callback');

			mod_assert.string(ctx.ctx_app.metadata.DOMAIN_NAME,
			    'DOMAIN_NAME');
			var resolver = 'nameservice.' +
			    ctx.ctx_app.metadata.DOMAIN_NAME;

			var cfg = {
				srvDomain: shard,
				cueballOptions: {
					resolvers: [
						resolver
					]
				},
				log: ctl.log.child({ moray: true }),
			};

			var c = mod_moray.createClient(cfg);

			c.once('connect', function () {
				callback(null, c);
			});
		},

		/*
		 * Finishing functions.  At most one of "retry", "hold", or
		 * "finish" should be called, and the selected function must be
		 * called at most once.
		 */
		retry: function (err) {
			assert_one_call();

			log.warn(err, 'plan "%s" phase "%s" failed (retrying)',
			    self.pr_plan.uuid, self.pr_plan.phase);
			setTimeout(function () {
				self.pr_running = false;
				self.dispatch();
			}, 5000);
		},
		hold: function (err) {
			assert_one_call();

			log.warn(err, 'plan "%s" phase "%s" holding execution',
			    self.pr_plan.uuid, self.pr_plan.phase);

			self.hold(err);
		},
		finish: function () {
			assert_one_call();

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

PlanRun.prototype.hold = function
hold(err)
{
	var self = this;

	mod_assert.ok(err instanceof Error, 'err must be an Error');
	mod_assert.ok(self.pr_running, 'must be running');

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

	lib_data_access.plan_store(self.pr_exe.exe_ctx, self.pr_plan,
	    function (err, newplan) {
		if (err) {
			log.warn(err, 'could not commit plan "%s" (retrying)',
			    self.pr_plan.uuid);
			self.pr_commit = false;
			setTimeout(function () {
				self.commit(callback);
			}, 5000);
			return;
		}

		log.info('plan "%s" committed (etag %s -> %s)',
		    self.pr_plan.uuid, old_etag, newplan._etag);
		self.pr_plan = newplan;

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

			plan.shard = plan_opts.shard;
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
