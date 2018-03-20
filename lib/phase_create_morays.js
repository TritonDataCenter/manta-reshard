/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;


function
phase_create_morays(ctl)
{
	var p = ctl.plan();
	var status = ctl.status();
	var status2 = ctl.status();

	var image = null;
	var servers = mod_jsprim.deepCopy(p.servers);
	servers.forEach(function (s) {
		s.moray_zone = null;
		s.status = status.child();
	});
	var sentinel, sentinel_etag;

	/*
	 * XXX We'll use the first image we see in an existing Moray instance
	 * for the source shard.
	 */

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Collect the list of Moray instances for the target shard.
		 */
		status.update('listing instances of "moray"');
		status.prop('shard', p.shard);
		ctl.get_instances({ service: 'moray', shard: p.shard },
		    function (err, _insts) {
			if (err) {
				done(err);
				return;
			}

			var uuids = Object.keys(_insts);
			if (uuids.length < 1) {
				done(new VE('no Moray instances found for ' +
				    'existing shard "%s"', p.shard));
				return;
			}

			image = _insts[uuids[0]].image_uuid;
			mod_assert.uuid(image, 'image');

			done();
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Collect the list of Moray instances for the new shard and
		 * associate them with the servers in the deployment plan.
		 */
		status.clear();
		status.update('listing instances of "moray"');
		status.prop('shard', p.new_shard);
		ctl.get_instances({ service: 'moray', shard: p.new_shard },
		    done);

	}, function (_insts, done) {
		var errors = [];

		mod_jsprim.forEachKey(_insts, function (uuid, i) {
			mod_assert.object(i.params.tags, 'params.tags');

			var t = i.params.tags;

			if (!t.manta_reshard_plan) {
				errors.push(new VE('Moray instance %s ' +
				    'does not have correct tag'));
				return;
			}

			if (t.manta_reshard_plan !== p.uuid) {
				errors.push(new VE('possible plan conflict ' +
				    'instance %s', uuid));
				return;
			}

			var this_server = i.params.server_uuid;
			mod_assert.uuid(this_server, 'params.server_uuid');

			var assigned = false;
			for (var j = 0; j < servers.length; j++) {
				if (servers[j].moray_zone !== null) {
					continue;
				}

				if (servers[j].uuid !== this_server) {
					continue;
				}

				servers[j].moray_zone = uuid;
				assigned = true;
				break;
			}

			if (!assigned) {
				errors.push(new VE('instance "%s" not for ' +
				    'a server in the plan', uuid));
				return;
			}
		});

		setImmediate(done, VE.errorFromList(errors));

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Check each server in the plan.  If the server does not have
		 * a Manatee peer, we need to provision one.
		 */
		status.clear();
		status.update('provisioning Moray instances');
		status.prop('shard', p.new_shard);
		mod_vasync.forEachPipeline({ inputs: servers,
		    func: function (server, next) {
			if (ctl.pausing(next)) {
				return;
			}

			create_one_moray(ctl, {
				server: server,
				image: image,
			}, next);

		}}, function (err) {
			done(err);
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Get a Moray client for the new shard.  We need not manage
		 * the life cycle of this client; the Executor will close it
		 * when the plan completes.
		 */
		status2.update('connecting new Moray client');
		ctl.new_shard_moray(done);

	}, function (client, done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Update the sentinel object in the new shard.
		 */
		sentinel = lib_common.create_sentinel_object();

		ctl.log.info({ sentinel: sentinel }, 'sentinel object');

		status2.update('updating sentinel object');
		client.putObject('manta_reshard_sentinel',
		    'sentinel', sentinel, function (err, res) {
			if (err) {
				done(new VE('updating sentinel'));
				return;
			}

			mod_assert.string(res.etag, 'res.etag');
			sentinel_etag = res.etag;

			done();
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Use the PostgreSQL IP for the async peer to check for
		 * replication of the sentinel update.
		 */
		var postgres_ip = ctl.prop_get('new_async_ip');

		status2.update('verifying replication of sentinel');
		lib_common.check_for_sentinel(ctl, { sentinel: sentinel,
		    sentinel_etag: sentinel_etag, postgres_ip: postgres_ip },
		    done);

	} ], function (err) {
		if (err) {
			ctl.retry(err);
			return;
		}

		status2.update('new shard ok');

		ctl.finish();
	});
}

function
create_one_moray(ctl, opts, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.object(opts, 'opts');
	mod_assert.func(callback, 'callback');

	mod_assert.object(opts.server, 'opts.server');
	var server = opts.server;

	mod_assert.uuid(opts.image, 'opts.image');
	var image = opts.image;

	var p = ctl.plan();


	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		server.status.clear();
		server.status.prop('dc', server.dc_name);
		server.status.prop('server', server.uuid);

		if (server.moray_zone !== null) {
			server.status.update('zone %s', server.moray_zone);
			server.status.child().update('found previously ' +
			    'provisioned zone');
			ctl.log.info('server %s in %s already has ' +
			    'peer %s', server.uuid, server.dc_name,
			    server.moray_zone);
			done();
			return;
		}

		server.status.update('provisioning new zone');

		ctl.log.info('provision new moray on server %s in %s',
		    server.uuid, server.dc_name);

		ctl.create_instance({
			service: 'moray',
			shard: ctl.new_short_shard(),
			server_uuid: server.uuid,
			image_uuid: image,
			datacenter_name: server.dc_name,
			tags: {
				manta_reshard_plan: p.uuid
			}
		}, function (err, inst) {
			if (err) {
				done(new VE(err, 'creating instance'));
				return;
			}

			ctl.log.info({ instance: inst },
			    'created instance "%s" in DC %s',
			    inst.uuid, server.dc_name);

			mod_assert.strictEqual(server.moray_zone, null,
			    'moray_zone set twice?');
			server.moray_zone = inst.uuid;

			server.status.update('zone %s', server.moray_zone);
			server.status.trunc();
			server.status.child().update('provisioned ok');

			done();
		});

	} ], function (err) {
		if (err) {
			callback(new VE(err, 'provisioning moray on %s',
			    server.uuid));
			return;
		}

		callback();
	});
}


module.exports = {
	phase_create_morays: phase_create_morays,
};
