

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

/*
 * Pass the name of a SAPI service in the Manta application; e.g., "postgres"
 * or "moray".  Returns an object which contains the set of instances, keyed on
 * the instance UUID.
 */
function
get_instances(ctx, filter, callback)
{
	mod_assert.object(filter, 'filter');
	mod_assert.string(filter.service, 'filter.service');
	mod_assert.optionalString(filter.shard, 'filter.shard');

	ctx.ctx_sapi.getApplicationObjects(ctx.ctx_app.uuid,
	    { include_master: true }, function (err, res) {
		if (err) {
			callback(new VE(err, 'load Manta application objects'));
			return;
		}

		mod_assert.object(res.services, 'services');
		mod_assert.object(res.instances, 'instances');

		var found = {};
		mod_jsprim.forEachKey(res.services, function (uuid, service) {
			if (service.name === filter.service) {
				mod_assert.arrayOfObject(res.instances[uuid],
				    'res.instances[' + uuid + ']');

				res.instances[uuid].forEach(function (i) {
					mod_assert.uuid(i.params.server_uuid,
					    'i.params.server_uuid');

					if (filter.hasOwnProperty('shard') &&
					    filter.shard !==
					    i.metadata.SERVICE_NAME) {
						return;
					}

					if (filter.hasOwnProperty('server') &&
					    filter.server !==
					    i.params.server_uuid) {
						return;
					}

					if (!filter.hasOwnProperty('shard') ||
					    filter.shard ===
					    i.metadata.SERVICE_NAME) {
						found[i.uuid] = i;
					}
				});
			}
		});

		setImmediate(callback, null, found);
	});
}

/*
 * Pass the name of a SAPI service in the Manta application; e.g., "postgres".
 * Returns the SAPI service object.
 */
function
get_service(ctx, name, callback)
{
	mod_assert.string(name, 'name');

	ctx.ctx_sapi.listServices({
		include_master: true,
		application_uuid: ctx.ctx_app.uuid,
		name: name
	}, function (err, res) {
		if (err) {
			callback(new VE(err, 'load service "%s"', name));
			return;
		}

		if (res.length !== 1) {
			callback(new VE(err, 'found %d services named "%s"',
			    name));
			return;
		}

		callback(null, res[0]);
	});
}

function
update_vmapi_vm(ctx, uuid, payload, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.uuid(uuid, 'uuid');
	mod_assert.object(payload, 'payload');
	mod_assert.func(callback, 'callback');

	find_zone(ctx, uuid, function (err, loc) {
		if (err) {
			callback(err);
			return;
		}

		var vmapi = ctx.ctx_dcs[loc.dc].dc_clients.dcc_vmapi;

		vmapi.updateVm({ uuid: uuid, payload: payload }, callback);
	});
}

function
find_zone(ctx, zone_uuid, callback)
{
	ctx.ctx_sapi.getInstance(zone_uuid, function (err, res) {
		if (err) {
			callback(new VE(err, 'could not locate zone "%s"',
			    zone_uuid));
			return;
		}

		mod_assert.object(res, 'res');
		mod_assert.object(res.params, 'res.params');
		mod_assert.uuid(res.params.server_uuid,
		    'res.params.server_uuid');
		mod_assert.object(res.metadata, 'res.metadata');
		mod_assert.string(res.metadata.DATACENTER,
		    'res.metadata.DATACENTER');

		var location = {
			dc: res.metadata.DATACENTER,
			server: res.params.server_uuid
		};

		ctx.ctx_log.info('found zone "%s" on server "%s" in DC "%s"',
		    zone_uuid, location.server, location.dc);

		callback(null, location);
	});
}

function
zone_exec(ctx, zone_uuid, script, callback)
{
	var log = ctx.ctx_log;

	var m = 'DONTUSETHISVALUEINYOURSCRIPT';
	var send_script = [
		'#!/bin/bash',
		'',
		'/usr/sbin/zlogin -Q ' + zone_uuid + ' bash -l <<\'' + m + '\'',
		script,
		m,
		'rv=$?',
		'',
		'if (( rv == 113 )); then',
		'        exit 1',
		'else',
		'        exit $rv',
		'fi',
	].join('\n');

	var loc, urc;
	mod_vasync.waterfall([ function (done) {
		find_zone(ctx, zone_uuid, done);

	}, function (_loc, done) {
		loc = _loc;

		/*
		 * 2. Run Ur discovery for the server we need.
		 */
		urc = ctx.ctx_dcs[loc.dc].dc_clients.dcc_urclient;

		var disco = urc.discover({ timeout: 4000,
		    exclude_headnode: false, node_list: [ loc.server ]});
		var found = false;

		disco.on('error', function (err) {
			done(new VE(err, 'ur discovery (DC %s)', loc.dc));
		});
		disco.on('server', function (s) {
			mod_assert.strictEqual(loc.server, s.uuid,
			    'uuid mismatch');

			log.info('discovered server %s (%s) in DC %s',
			    s.uuid, s.hostname, loc.dc);

			found = true;
		});
		disco.on('end', function () {
			if (!found) {
				done(new VE('discovery did not find "%s"',
				    loc.server));
				return;
			}

			done();
		});

	}, function (done) {
		/*
		 * 3. Run script on remote host.
		 */
		urc.exec({ server_uuid: loc.server, timeout: 300 * 1000,
		    script: send_script }, function (err, res) {
			if (err) {
				done(new VE(err, 'ur exec (server %s)',
				    loc.server));
				return;
			}

			done(null, res);
		});

	} ], function (err, res) {
		if (err) {
			callback(new VE(err, 'zone_exec(%s)', zone_uuid));
			return;
		}

		callback(null, res);
	});
}

module.exports = {
	refresh_manta_application: refresh_manta_application,
	find_server: find_server,
	get_instances: get_instances,
	get_service: get_service,
	zone_exec: zone_exec,
	update_vmapi_vm: update_vmapi_vm,
};
