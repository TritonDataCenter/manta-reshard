
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var VE = mod_verror.VError;


function
phase_create_manatee_peers(ctl)
{
	var p = ctl.plan();

	mod_assert.equal(p.split_count, 2, 'split_count must be 2 for now');
	mod_assert.string(p.shard, 'p.shard');
	mod_assert.equal(p.servers.length, (p.split_count - 1) * 3,
	    'must be three servers for each target shard');

	var insts;
	var base_peer_image = null;
	var base_peers = [];
	var servers = mod_jsprim.deepCopy(p.servers);
	servers.forEach(function (s) {
		s.manatee_peer = null;
	});

	/*
	 * First, all instances for the shard we are splitting up.  If this
	 * phase has partially run already and been subsequently restarted,
	 * there might be relevant instances we have already provisioned.
	 */
	mod_vasync.waterfall([ function (done) {
		ctl.get_instances({ service: 'postgres', shard: p.shard },
		    function (err, _insts) {
			if (err) {
				done(err);
				return;
			}

			insts = _insts;
			done();
		});

	}, function (done) {
		/*
		 * Check that we have the three base peers that were not
		 * created as part of this resharding plan.
		 */
		var errors = [];

		mod_jsprim.forEachKey(insts, function (uuid, i) {
			mod_assert.object(i.params.tags, 'params.tags');

			var t = i.params.tags;

			if (!t.manta_reshard_plan) {
				mod_assert.uuid(i.params.image_uuid,
				    'image_uuid');

				/*
				 * Make sure that all existing peers are using
				 * a consistent Manatee image.
				 */
				if (base_peer_image !== null &&
				    base_peer_image !== i.params.image_uuid) {
					errors.push(new VE('base peers ' +
					    'on inconsistent images'));
				}
				base_peer_image = i.params.image_uuid;

				base_peers.push(i.uuid);
				return;
			}

			if (t.manta_reshard_plan !== p.uuid) {
				errors.push(new VE('possible conflict: ' +
				    'instance %s', uuid));
				return;
			}

			var this_server = i.params.server_uuid;
			mod_assert.uuid(this_server, 'params.server_uuid');

			/*
			 * Assign this Manatee peer to one of the server slots
			 * in the plan.  If there is no free slot, this server
			 * should not have received a Manatee peer!
			 */
			var assigned = false;
			for (var j = 0; j < servers.length; j++) {
				if (servers[j].manatee_peer !== null) {
					continue;
				}

				if (servers[j].uuid !== this_server) {
					continue;
				}

				servers[j].manatee_peer = uuid;
				assigned = true;
				break;
			}

			if (!assigned) {
				errors.push(new VE('instance "%s" not for ' +
				    'a server in the plan', uuid));
				return;
			}
		});

		if (base_peers.length !== 3) {
			errors.push(new VE('found %d base peers instead of 3',
			    base_peers.length));
		}
		mod_assert.uuid(base_peer_image, 'base_peer_image');

		ctl.log.info({ base_peer_image: base_peer_image,
		    base_peers: base_peers }, 'base peers');
		ctl.log.info({ servers: servers }, 'current server status');

		setImmediate(done, VE.errorFromList(errors));

	}, function (done) {
		/*
		 * Check each server in the plan.  If the server does not have
		 * a Manatee peer, we need to provision one.
		 */
		mod_vasync.forEachPipeline({ inputs: servers,
		    func: function (server, next) {
			if (server.manatee_peer !== null) {
				ctl.log.info('server %s in %s already has ' +
				    'peer %s', server.uuid, server.dc_name,
				    server.manatee_peer);
				next();
				return;
			}

			ctl.log.info('provision new peer on server %s in %s',
			    server.uuid, server.dc_name);

			ctl.create_instance({
				service: 'postgres',
				shard: ctl.short_shard(),
				server_uuid: server.uuid,
				image_uuid: base_peer_image,
				datacenter_name: server.dc_name,
				tags: {
					manta_reshard_plan: p.uuid
				}
			}, function (err, inst) {
				if (err) {
					next(new VE(err, 'creating instance'));
					return;
				}

				ctl.log.info({ instance: inst },
				    'created instance "%s" in DC %s',
				    inst.uuid, server.dc_name);

				next();
			});

		}}, function (err) {
			done(err);
		});

	} ], function (err) {
		if (err) {
			ctl.log.warn(err, 'phase failed; retry');
			setTimeout(function () {
				phase_create_manatee_peers(ctl);
			}, 5000);
			return;
		}

		ctl.finish();
	});
}

module.exports = {
	phase_create_manatee_peers: phase_create_manatee_peers,
};
