

var mod_assert = require('assert-plus');
var mod_restify = require('restify');
var mod_verror = require('verror');
var mod_jsprim = require('jsprim');

var VE = mod_verror.VError;


function
handle_ping(req, res, next)
{
	req.log.debug('ping request');

	res.send(200, { ok: true });
	next();
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

	s.get('/ping', handle_ping);

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
