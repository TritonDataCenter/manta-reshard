/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_net = require('net');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var lib_common = require('../lib/common');

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
	mod_assert.optionalUuid(filter.server, 'filter.server');

	mod_vasync.waterfall([ function gis_get_service(done) {
		get_service(ctx, filter.service, done);

	}, function gis_list_instances(service, done) {
		mod_assert.uuid(service.uuid, 'service.uuid');

		/*
		 * Load all instances of the nominated service.
		 */
		ctx.ctx_sapi.listInstances({
			include_master: true,
			service_uuid: service.uuid
		}, function (err, res) {
			if (err) {
				done(new VE(err, 'load instances of service ' +
				    '"%s"', filter.service));
				return;
			}

			mod_assert.arrayOfObject(res, 'res');
			done(null, res);
		});

	}, function gis_filter_instances_shard(instances, done) {
		mod_assert.arrayOfObject(instances, 'instances');
		mod_assert.func(done, 'done');

		if (filter.hasOwnProperty('shard')) {
			instances = instances.filter(function (i) {
				return (filter.shard ===
				    i.metadata.SERVICE_NAME);
			});
		}

		setImmediate(done, null, instances);

	}, function gis_vmapi_augment(instances, done) {
		/*
		 * SAPI does not, itself, keep the contents of "params" current
		 * when the operator uses "sapiadm reprovision" to update a
		 * zone, or if a zone is manually migrated to another server.
		 * As such, we fetch the current server and image UUID from
		 * VMAPI and ignore the values in "params".
		 */
		mod_vasync.forEachPipeline({ inputs: instances,
		    func: function gis_vmapi_augment_one(i, next) {
			var dc = i.metadata.DATACENTER;
			mod_assert.string(dc, 'i.metadata.DATACENTER');

			var vmapi = ctx.ctx_dcs[dc].dc_clients.dcc_vmapi;

			vmapi.getVm({ uuid: i.uuid }, function (err, vm) {
				if (err) {
					next(new VE(err, 'augment instance ' +
					    '%s from VMAPI (%s)', i.uuid, dc));
					return;
				}

				if (!lib_common.is_uuid(vm.image_uuid)) {
					next(new VE('VMAPI (%s) has no ' +
					    'image UUID for instance %s', dc,
					    i.uuid));
					return;
				}
				i.image_uuid = vm.image_uuid;

				if (!lib_common.is_uuid(vm.server_uuid)) {
					next(new VE('VMAPI (%s) has no ' +
					    'server UUID for instance %s', dc,
					    i.uuid));
					return;
				}
				i.server_uuid = vm.server_uuid;

				next();
			});

		}}, function (err) {
			done(err, instances);
		});

	}, function gis_filter_instances_server(instances, done) {
		mod_assert.arrayOfObject(instances, 'instances');
		mod_assert.func(done, 'done');

		if (filter.hasOwnProperty('server')) {
			instances = instances.filter(function (i) {
				mod_assert.uuid(i.server_uuid, 'i.server_uuid');

				return (filter.server === i.server_uuid);
			});
		}

		setImmediate(done, null, instances);

	} ], function (err, instances) {
		if (err) {
			callback(new VE(err, 'get instances matching %j',
			    filter));
			return;
		}

		var found = {};
		instances.forEach(function (i) {
			mod_assert.uuid(i.uuid, 'i.uuid');

			found[i.uuid] = i;
		});

		callback(null, found);
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

		/*
		 * XXX "updateVM()" essentially returns a workflow job ID,
		 * which we should then poll for completion.
		 */
		vmapi.updateVm({ uuid: uuid, payload: payload }, callback);
	});
}

/*
 * Get the IP address of the primary NIC for this zone.
 */
function
get_instance_ip(ctx, uuid, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.uuid(uuid, 'uuid');
	mod_assert.func(callback, 'callback');

	find_zone(ctx, uuid, function (err, loc) {
		if (err) {
			callback(err);
			return;
		}

		var vmapi = ctx.ctx_dcs[loc.dc].dc_clients.dcc_vmapi;

		vmapi.getVm({ uuid: uuid }, function (err, vm) {
			if (err) {
				callback(err);
				return;
			}

			for (var i = 0; i < vm.nics.length; i++) {
				var nic = vm.nics[i];

				if (nic.primary) {
					mod_assert.ok(mod_net.isIPv4(nic.ip,
					    'nic.ip'));

					callback(null, nic.ip);
					return;
				}
			}

			callback(new VE('VM "%s" had no primary NIC', uuid));
		});
	});
}

function
get_instance(ctx, zone_uuid, callback)
{
	ctx.ctx_sapi.getInstance(zone_uuid, function (err, res) {
		if (err) {
			callback(new VE(err, 'could not locate zone "%s"',
			    zone_uuid));
			return;
		}

		mod_assert.object(res, 'res');
		mod_assert.object(res.params, 'res.params');
		mod_assert.object(res.metadata, 'res.metadata');

		callback(null, res);
	});
}

function
find_zone(ctx, zone_uuid, callback)
{
	get_instance(ctx, zone_uuid, function (err, res) {
		if (err) {
			callback(err);
			return;
		}

		mod_assert.uuid(res.params.server_uuid,
		    'res.params.server_uuid');
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
		 * Select the Ur client for this DC.
		 */
		urc = ctx.ctx_dcs[loc.dc].dc_clients.dcc_urclient;
		if (!urc.ready()) {
			done(new VE('ur client (dc %s) not ready', loc.dc));
			return;
		}

		/*
		 * Before we run discovery, check to see if this server
		 * is in the discovery cache.
		 */
		var udc = ctx.ctx_ur_disco_cache[loc.server];
		if (udc) {
			mod_assert.strictEqual(loc.dc, udc.udc_dc,
			    'cache entry must match expected DC');
			mod_assert.uuid(udc.udc_uuid, 'udc_uuid');
			mod_assert.string(udc.udc_hostname, 'udc_hostname');
			mod_assert.number(udc.udc_used, 'udc_used');

			udc.udc_used++;

			log.info('cached server %s (%s) in DC %s (use #%d)',
			    udc.udc_uuid, udc.udc_hostname, loc.dc,
			    udc.udc_used);
			setImmediate(done);
			return;
		}

		/*
		 * Run discovery to ensure the target server is available.
		 */
		var disco = urc.discover({ timeout: 4000,
		    exclude_headnode: false });
		var found = false;

		disco.on('error', function (err) {
			done(new VE(err, 'ur discovery (DC %s)', loc.dc));
		});
		disco.on('server', function (s) {
			log.info('discovered server %s (%s) in DC %s',
			    s.uuid, s.hostname, loc.dc);

			/*
			 * Though we are performing this discovery to look for
			 * a particular server, we should cache any servers that
			 * happen to respond.  This will substantially cut down
			 * on the number of discovery operations we need to do
			 * after the first one.
			 */
			udc = ctx.ctx_ur_disco_cache[s.uuid];
			if (!udc) {
				ctx.ctx_ur_disco_cache[s.uuid] = {
					udc_uuid: s.uuid,
					udc_hostname: s.hostname,
					udc_dc: loc.dc,
					udc_last_seen: process.hrtime(),
					udc_used: 0,
				};
			} else {
				/*
				 * We have seen this server before; update the
				 * cache entry time stamp to prevent expiry.
				 */
				udc.udc_last_seen = process.hrtime();
			}

			if (loc.server === s.uuid) {
				found = true;
			}
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
		 * Run script on remote host.
		 */
		urc.exec({ server_uuid: loc.server, timeout: 300 * 1000,
		    script: send_script }, function (err, res) {
			if (err) {
				var info = {};

				if (err.message.match(/^timeout for host /)) {
					/*
					 * XXX It would be better if the Ur
					 * client was itself able to provide a
					 * property like this.
					 */
					info.exec_timeout = true;
				}

				done(new VE({ info: info, cause: err },
				    'ur exec (server %s)', loc.server));
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

/*
 * List images from the IMGAPI service designated for hash ring image storage.
 */
function
list_hashring_images(ctx, opts, callback)
{
	ctx.ctx_hashring_imgapi.listImages(opts, function (err, images, res) {
		callback(err, images);
	});
}

function
get_hashring_image(ctx, uuid, callback)
{
	ctx.ctx_hashring_imgapi.getImage(uuid, function (err, image, res) {
		callback(err, image);
	});
}


module.exports = {
	get_sapi_application: get_sapi_application,
	refresh_manta_application: refresh_manta_application,
	find_server: find_server,
	get_instance: get_instance,
	get_instances: get_instances,
	get_service: get_service,
	zone_exec: zone_exec,
	update_vmapi_vm: update_vmapi_vm,
	get_instance_ip: get_instance_ip,
	list_hashring_images: list_hashring_images,
	get_hashring_image: get_hashring_image,
};
