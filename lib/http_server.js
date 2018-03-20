/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_assert = require('assert-plus');
var mod_restify = require('restify');
var mod_verror = require('verror');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;


function
handle_ping(req, res, next)
{
	var dt = new Date();

	req.log.debug('ping request');

	var dcs = [];

	mod_jsprim.forEachKey(req.ctx.ctx_dcs, function (dcname, dc) {
		dcs.push({
			region: dc.dc_region,
			name: dc.dc_name,
			sapi_url: dc.dc_sapi,
			rabbitmq: dc.dc_app.metadata.rabbitmq,
		});
	});

	res.send(200, {
		ok: true,
		when: dt.toISOString(),
		datacenters: dcs,
	});
	next();
}

function
handle_phases(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('phase list request');

	res.send(200, { phases: ctx.ctx_exec.list_phases() });
	setImmediate(next);
}

function
handle_plans(req, res, next)
{
	var ctx = req.ctx;
	var exe = ctx.ctx_exec;

	req.log.debug('plan list request');

	/*
	 * XXX We should likely wait until we're sure all plans are initially
	 * loaded from the database before answering this request.
	 */

	var plans = {};

	mod_jsprim.forEachKey(exe.exe_plan_run, function (uuid, pr) {
		plans[uuid] = {
			uuid: uuid,
			shard: pr.pr_plan.shard,
			new_shard: pr.pr_plan.new_shard,
			running: pr.pr_running,
			phase: pr.pr_plan.phase,
			held: pr.pr_plan.hold !== null,
			retrying: pr.pr_retry_info !== null,
			status: pr.pr_status.dump(),
			props: pr.pr_plan.props,
			tuning: pr.pr_plan.tuning,
		};

		if (plans[uuid].held) {
			plans[uuid].error = pr.pr_plan.hold;
		} else if (plans[uuid].retrying) {
			plans[uuid].error = pr.pr_retry_info;
		}
	});

	res.send(200, { plans: plans, problems: exe.exe_plan_problems });
	setImmediate(next);
}

function
handle_plan(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('plan request');

	if (!req.body.plan_opts) {
		res.send(400, { error: 'no "plan_opts"' });
		next();
		return;
	}

	ctx.ctx_exec.create_plan(req.body.plan_opts,
	    function (err, plan) {
		if (err) {
			var i = VE.info(err);

			req.log.error(err, 'plan error');
			res.send(500, {
				error: err.message,
				info: i
			});
			next();
			return;
		}

		res.send(200, plan);
		next();
	});
}

function
handle_plan_unhold(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('plan unhold request');

	ctx.ctx_exec.handle_unhold({ plan_uuid: req.plan_uuid },
	    function (err) {
		if (err) {
			next(new VE(err, 'unhold error'));
			return;
		}

		res.send(200, { ok: true });
		next();
	});
}

function
handle_plan_archive(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('plan archive request');

	ctx.ctx_exec.handle_archive({ plan_uuid: req.plan_uuid },
	    function (err) {
		if (err) {
			next(new VE(err, 'archive error'));
			return;
		}

		res.send(200, { ok: true });
		next();
	});
}


function
handle_plan_resume(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('plan resume request');

	ctx.ctx_exec.handle_resume({ plan_uuid: req.plan_uuid },
	    function (err) {
		if (err) {
			next(new VE(err, 'resume error'));
			return;
		}

		res.send(200, { ok: true });
		next();
	});
}

function
handle_plan_pause(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('plan pause request');

	var opts = { plan_uuid: req.plan_uuid };

	if (req.body) {
		var pap = req.body.pause_at_phase;

		if (pap !== undefined && pap !== null &&
		    typeof (pap) !== 'string') {
			next(new VE('"pause_at_phase" must be string ' +
			    'or null or not present'));
			return;
		}

		if (pap !== undefined) {
			opts.pause_at_phase = pap;
		}
	}

	ctx.ctx_exec.handle_pause(opts, function (err) {
		if (err) {
			next(new VE(err, 'pause error'));
			return;
		}

		res.send(200, { ok: true });
		next();
	});
}

function
handle_update(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('status update request');

	var token = req.params.token;
	if (typeof (token) !== 'string' || token === '') {
		next(new VE('token invalid'));
		return;
	}

	ctx.ctx_exec.handle_update({ plan_uuid: req.plan_uuid, token: token,
	    body: req.body }, function (err) {
		if (err) {
			next(new VE(err, 'handler returned error'));
			return;
		}

		res.send(200);
		next();
	});
}

function
handle_plan_tune(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('plan tune get request');

	var pr = ctx.ctx_exec.exe_plan_run[req.plan_uuid];
	if (!pr) {
		setImmediate(next, new VE('plan "%s" is not running',
		    req.plan_uuid));
		return;
	}

	mod_assert.object(pr.pr_plan.tuning, 'tuning on plan ' + req.plan_uuid);
	res.send(200, pr.pr_plan.tuning);
	setImmediate(next);
}

function
handle_plan_tune_post(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('plan tune set request');

	var tuning_name = req.params.tuning_name;
	if (typeof (tuning_name) !== 'string' || tuning_name === '') {
		next(new VE('tuning_name invalid'));
		return;
	}

	if (!req.body || !req.body.hasOwnProperty('tuning_value')) {
		next(new VE('tuning_value missing'));
		return;
	}

	var tuning_value = req.body.tuning_value;
	if (tuning_value !== null && typeof (tuning_value) !== 'number') {
		next(new VE('tuning_value must be a number or null'));
		return;
	}

	ctx.ctx_exec.handle_tune({ plan_uuid: req.plan_uuid,
	    tuning_name: tuning_name, tuning_value: tuning_value },
	    function (err) {
		if (err) {
			next(new VE(err, 'tune error'));
			return;
		}

		res.send(200, { ok: true });
		next();
	});
}

function
pre_plan_uuid(req, res, next)
{
	if (!lib_common.is_uuid(req.params.plan_uuid)) {
		setImmediate(next, new VE('plan UUID invalid'));
		return;
	}

	req.plan_uuid = req.params.plan_uuid;
	setImmediate(next);
}

function
create_http_server(ctx, done)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.func(done, 'done');

	var port = 80;

	var clog = ctx.ctx_log.child({ component: 'restify' });

	var s = mod_restify.createServer({
		name: 'manta-reshard',
		version: '1.0.0',
		log: clog,
	});

	s.on('after', mod_restify.plugins.auditLogger({
		log: ctx.ctx_log.child({ component: 'restify/audit',
		    audit: true }),
		server: s,
		event: 'after',
	}));

	s.use(mod_restify.plugins.bodyParser({ rejectUnknown: false }));
	s.use(mod_restify.plugins.requestLogger());
	s.use(function (req, res, next) {
		req.ctx = ctx;
		setImmediate(next);
	});

	/*
	 * Ping does not require any resources to be fully loaded.
	 */
	s.get('/ping', handle_ping);

	/*
	 * All of the other endpoints require the Executor to be ready.
	 */
	s.use(function (req, res, next) {
		if (!req.ctx.ctx_exec) {
			next(new VE('executor not ready yet'));
			return;
		}
		if (!req.ctx.ctx_moray) {
			next(new VE('moray not ready yet'));
			return;
		}
		setImmediate(next);
	});

	s.get('/phases', handle_phases);
	s.post('/plan', handle_plan);
	s.post('/plan/:plan_uuid/pause', pre_plan_uuid, handle_plan_pause);
	s.post('/plan/:plan_uuid/resume', pre_plan_uuid, handle_plan_resume);
	s.post('/plan/:plan_uuid/archive', pre_plan_uuid, handle_plan_archive);
	s.post('/plan/:plan_uuid/unhold', pre_plan_uuid, handle_plan_unhold);
	s.get('/plan/:plan_uuid/tune', pre_plan_uuid, handle_plan_tune);
	s.post('/plan/:plan_uuid/tune/:tuning_name', pre_plan_uuid,
	    handle_plan_tune_post);
	s.get('/plans', handle_plans);
	s.post('/update/:plan_uuid/:token', pre_plan_uuid, handle_update);

	s.listen(port, function (err) {
		if (err) {
			done(new VE(err, 'restify listen on port %d', port));
			return;
		}

		ctx.ctx_restify = s;

		done();
	});
}


module.exports = {
	create_http_server: create_http_server,
};
