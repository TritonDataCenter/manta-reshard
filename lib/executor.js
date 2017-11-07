

var mod_assert = require('assert-plus');
var mod_restify = require('restify');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');
var mod_uuid = require('uuid');

var VE = mod_verror.VError;

var lib_triton_access = require('./triton_access');
var lib_data_access = require('./data_access');
var lib_serial = require('./serial');


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

	self.exe_plan_run = {};
	self.exe_switching = false;

	self.exe_phases = [
		'waiting_room',
		'holding_pattern'
	];

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

	self.exe_log.info('starting execution of plan "%s"', plan.uuid);

	var pr = {
		pr_phase: null,
		pr_plan: plan
	};

	self.exe_plan_run[plan.uuid] = pr;

	var next_phase = function () {
		if (pr.pr_phase === null) {
			pr.pr_phase = self.exe_phases[0];
		} else {
			pr.pr_phase = self.exe_phases[
			    self.exe_phases.indexOf(pr.pr_phase) + 1];
		}

		if (!pr.pr_phase) {
			/*
			 * Plan run complete!
			 */
			self.exe_log.info('plan "%s" complete!');
			return;
		}

		var ctl = {
			log: self.exe_log.child({
				plan: plan.uuid,
				phase: pr.pr_phase
			}),
			finish: next_phase,
		};

		var phase_func = require('./phase_' + pr.pr_phase + '.js')[
		    'phase_' + pr.pr_phase];

		phase_func(ctl);
	};

	next_phase();
};

Executor.prototype._swtch = function
_swtch()
{
	var self = this;

	if (self.exe_switching) {
		return;
	}
	self.exe_switching = true;

	var finish = function () {
		self.exe_switching = false;
		setTimeout(function () {
			self._swtch();
		}, 5000);
	};

	/*
	 * Load plans from database.
	 */
	lib_data_access.plans_active(self.exe_ctx, function (err, plans) {
		if (err) {
			self.exe_log.error(err, 'loading active plans');
			finish();
			return;
		}

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

		finish();
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
