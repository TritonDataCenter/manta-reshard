
var mod_path = require('path');
var mod_fs = require('fs');

var mod_assert = require('assert-plus');
var mod_restify = require('restify');
var mod_verror = require('verror');
var mod_jsprim = require('jsprim');

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

	req.log.debug('plan list request');

	if (!ctx.ctx_exec) {
		next(new VE('executor not ready yet'));
		return;
	}
	var exe = ctx.ctx_exec;

	/*
	 * XXX We should likely wait until we're sure all plans are initially
	 * loaded from the database before answering this request.
	 */

	var out = {};

	mod_jsprim.forEachKey(exe.exe_plan_run, function (uuid, pr) {
		out[uuid] = {
			uuid: uuid,
			running: pr.pr_running,
			phase: pr.pr_plan.phase,
			held: pr.pr_plan.hold !== null,
			retrying: pr.pr_retry_info !== null,
			status: pr.pr_status.dump()
		};

		if (out[uuid].held) {
			out[uuid].error = pr.pr_plan.hold;
		} else if (out[uuid].retrying) {
			out[uuid].error = pr.pr_retry_info;
		}
	});

	res.send(200, { plans: out });
	setImmediate(next);
}

function
handle_plan(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('plan request');

	if (!ctx.ctx_exec) {
		next(new VE('executor not ready yet'));
		return;
	}
	if (!ctx.ctx_moray) {
		next(new VE('moray not ready yet'));
		return;
	}

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
handle_plan_resume(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('plan resume request');

	if (!ctx.ctx_exec) {
		next(new VE('executor not ready yet'));
		return;
	}
	if (!ctx.ctx_moray) {
		next(new VE('moray not ready yet'));
		return;
	}

	var plan_uuid = req.params.plan_uuid;
	if (plan_uuid.length !== 36) {
		/*
		 * XXX
		 */
		next(new VE('plan UUID invalid'));
		return;
	}

	ctx.ctx_exec.handle_resume({ plan_uuid: plan_uuid }, function (err) {
		if (err) {
			next(new VE(err, 'pause error'));
			return;
		}

		res.send(200);
		next();
	});
}

function
handle_plan_pause(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('plan pause request');

	if (!ctx.ctx_exec) {
		next(new VE('executor not ready yet'));
		return;
	}
	if (!ctx.ctx_moray) {
		next(new VE('moray not ready yet'));
		return;
	}

	var plan_uuid = req.params.plan_uuid;
	if (plan_uuid.length !== 36) {
		/*
		 * XXX
		 */
		next(new VE('plan UUID invalid'));
		return;
	}

	ctx.ctx_exec.handle_pause({ plan_uuid: plan_uuid }, function (err) {
		if (err) {
			next(new VE(err, 'pause error'));
			return;
		}

		res.send(200);
		next();
	});
}

function
handle_update(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('status update request');

	if (!ctx.ctx_exec) {
		next(new VE('executor not ready yet'));
		return;
	}

	var plan_uuid = req.params.plan_uuid;
	if (plan_uuid.length !== 36) {
		/*
		 * XXX
		 */
		next(new VE('plan UUID invalid'));
		return;
	}

	var token = req.params.token;
	if (!token) {
		/*
		 * XXX
		 */
		next(new VE('token invalid'));
		return;
	}

	ctx.ctx_exec.handle_update({ plan_uuid: plan_uuid, token: token,
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

	s.get('/ping', handle_ping);
	s.get('/phases', handle_phases);
	s.post('/plan', handle_plan);
	s.post('/plan/:plan_uuid/pause', handle_plan_pause);
	s.post('/plan/:plan_uuid/resume', handle_plan_resume);
	s.get('/plans', handle_plans);
	s.post('/update/:plan_uuid/:token', handle_update);

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
