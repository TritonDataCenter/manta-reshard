

var mod_assert = require('assert-plus');
var mod_moray = require('moray');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var VE = mod_verror.VError;

function
get_sapi_application(log, sapi, name, include_master, callback)
{
	mod_assert.object(log, 'log');
	mod_assert.object(sapi, 'sapi');
	mod_assert.string(name, 'name');
	mod_assert.bool(include_master, 'include_master');
	mod_assert.func(callback, 'callback');

	var opts = {
		name: name
	};

	if (include_master) {
		/*
		 * Note that SAPI merely checks for the _presence_ of
		 * "include_master" in the query string.  Passing any value,
		 * even "false", results in the inclusion of master results.
		 */
		opts.include_master = true;
	}

	sapi.listApplications(opts, function (err, apps) {
		if (err) {
			callback(new VE(err, 'locate "%s" application', name));
			return;
		}

		if (apps.length !== 1) {
			callback(new VE('found %d "%s" applications, wanted 1',
			    apps.length, name));
			return;
		}

		callback(null, apps[0]);
	});
}

/*
 * Check all of the DCs we know about to find this server.  Ensure that it
 * appears in exactly one DC; no more, no less.
 */
function
find_server(ctx, uuid, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.uuid(uuid, 'uuid');
	mod_assert.func(callback, 'callback');

	var matches = [];

	mod_vasync.forEachParallel({ inputs: Object.keys(ctx.ctx_dcs),
	    func: function (dc_name, done) {
		var dc = ctx.ctx_dcs[dc_name];
		var cnapi = dc.dc_clients.dcc_cnapi;

		cnapi.getServer(uuid, function (err, server) {
			if (err) {
				if (err.statusCode === 404) {
					done();
					return;
				}

				done(new VE(err, 'in DC "%s"', dc_name));
				return;
			}

			matches.push({
				dc_name: dc_name,
				uuid: uuid,
				hostname: server.hostname
			});

			done();
		});

	}}, function (err) {
		if (err) {
			callback(new VE(err, 'find server "%s"', uuid));
			return;
		}

		if (matches.length < 1) {
			callback(new VE('could not find server "%s"', uuid));
			return;
		}

		if (matches.length > 1) {
			callback(new VE('server "%s" in multiple DCs', uuid));
			return;
		}

		callback(null, matches[0]);
	});
}

function
refresh_manta_application(ctx, callback)
{
	get_sapi_application(ctx.ctx_log, ctx.ctx_sapi, 'manta', true,
	    function (err, app) {
		if (err) {
			callback(err);
			return;
		}

		ctx.ctx_app_refresh = Date.now();
		ctx.ctx_app = app;

		ctx.ctx_log.trace({ manta_app: app }, 'manta application');

		mod_assert.strictEqual(ctx.ctx_app.metadata.REGION,
		    ctx.ctx_cfg.region, 'REGION');

		callback(null, app);
	});
}

module.exports = {
	refresh_manta_application: refresh_manta_application,
	find_server: find_server,
};
