

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

	s.use(mod_restify.plugins.bodyParser({ rejectUnknown: true }));
	s.use(mod_restify.plugins.requestLogger());
	s.use(function (req, res, next) {
		req.ctx = ctx;
		setImmediate(next);
	});

	s.get('/ping', handle_ping);
	s.post('/plan', handle_plan);

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
