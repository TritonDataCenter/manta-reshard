
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');
var mod_pg = require('pg');

var VE = mod_verror.VError;

var lib_manatee_adm = require('../lib/manatee_adm');
var lib_common = require('../lib/common');


function
phase_create_manatee_peers(ctl)
{
	var p = ctl.plan();
	var status = ctl.status();

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
		s.status = status.child();
	});

	/*
	 * First, all instances for the shard we are splitting up.  If this
	 * phase has partially run already and been subsequently restarted,
	 * there might be relevant instances we have already provisioned.
	 */
	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		status.update('listing instances of "postgres"');
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

		status.update('checking peer layout');
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
		if (!lib_common.is_uuid(base_peer_image)) {
			errors.push(new VE('could not find base peer image'));
		}

		ctl.log.info({ base_peer_image: base_peer_image,
		    base_peers: base_peers }, 'base peers');
		ctl.log.info({ servers: servers }, 'current server status');

		setImmediate(done, VE.errorFromList(errors));

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Check each server in the plan.  If the server does not have
		 * a Manatee peer, we need to provision one.
		 */
		status.update('creating Manatee peers');
		mod_vasync.forEachPipeline({ inputs: servers,
		    func: function (server, next) {
			if (ctl.pausing(next)) {
				return;
			}

			create_one_peer(ctl, {
				server: server,
				base_peers: base_peers,
				base_peer_image: base_peer_image,
			}, next);

		}}, function (err) {
			done(err);
		});

	} ], function (err) {
		if (err) {
			ctl.retry(err);
			return;
		}

		ctl.finish();
	});
}

/*
 * When we create a peer, we want to be sure it has correctly provisioned and
 * that it joins the end of the replication chain.  To do this, we need to
 * update the sentinel object and wait for that change to become visible on the
 * new peer.
 */
function
create_one_peer(ctl, opts, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.object(opts, 'opts');
	mod_assert.func(callback, 'callback');

	mod_assert.object(opts.server, 'opts.server');
	var server = opts.server;

	mod_assert.uuid(opts.base_peer_image, 'opts.base_peer_image');
	var base_peer_image = opts.base_peer_image;

	mod_assert.arrayOfUuid(opts.base_peers, 'opts.base_peers');
	var base_peers = opts.base_peers;

	var p = ctl.plan();

	var sentinel, sentinel_etag;
	var postgres_ip;

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		if (server.manatee_peer !== null) {
			server.status.update('zone %s (dc %s, server %s)',
			    server.manatee_peer, server.dc_name, server.uuid);
			server.status.child().update('found previously ' +
			    'provisioned zone');
			ctl.log.info('server %s in %s already has ' +
			    'peer %s', server.uuid, server.dc_name,
			    server.manatee_peer);
			done();
			return;
		}

		server.status.update('provisioning new zone (dc %s, server %s)',
		    server.dc_name, server.uuid);

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
				done(new VE(err, 'creating instance'));
				return;
			}

			ctl.log.info({ instance: inst },
			    'created instance "%s" in DC %s',
			    inst.uuid, server.dc_name);

			mod_assert.strictEqual(server.manatee_peer, null,
			    'manatee_peer set twice?');
			server.manatee_peer = inst.uuid;

			server.status.update('zone %s (dc %s, server %s)',
			    server.manatee_peer, server.dc_name, inst.uuid);
			server.status.child().update('provisioned ok');

			done();
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Update the sentinel object.
		 */
		sentinel = lib_common.create_sentinel_object();

		var stch = server.status.child();

		ctl.log.info({ sentinel: sentinel }, 'sentinel object');

		stch.update('updating sentinel object');
		ctl.target_moray().putObject('manta_reshard_sentinel',
		    'sentinel', sentinel, function (err, res) {
			if (err) {
				done(new VE('updating sentinel'));
				return;
			}

			mod_assert.string(res.etag, 'res.etag');
			sentinel_etag = res.etag;

			stch.done();
			done();
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Wait for the new instance to be visible in the Manatee
		 * cluster as an "async" peer.
		 */
		var stch = server.status.child();
		var check_for_peer = function () {
			stch.update('waiting for peer to join cluster');
			lib_common.manatee_adm_show(ctl, base_peers[0],
			    function (err, show) {
				if (err) {
					ctl.log.info(err, 'retrying');
					/* XXX */
					setTimeout(check_for_peer, 5000);
					return;
				}

				var zp = show.peers.filter(function (p) {
					return (p.uuid === server.manatee_peer);
				});

				if (zp.length < 1) {
					ctl.log.info('waiting for peer');
					setTimeout(check_for_peer, 5000);
					return;
				}

				if (zp[0].role !== 'async' ||
				    zp[0].pg !== 'ok' ||
				    zp[0].repl !== 'async') {
					ctl.log.info('peer not OK yet');
					setTimeout(check_for_peer, 5000);
					return;
				}

				postgres_ip = zp[0].ip;

				stch.done();
				ctl.log.info({ peer: zp[0] }, 'peer OK!');
				done();
			});
		};

		check_for_peer();

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Now that we have seen the peer correctly in the cluster,
		 * attempt to read our sentinel value from it.  We need to do
		 * this using a raw PostgreSQL connection, as Moray will only
		 * ever read from the primary peer in the cluster.
		 */
		var stch = server.status.child();
		var check_for_sentinel = function () {
			var client;

			var close_client = function (xx) {
				if (!client) {
					setImmediate(xx);
					return;
				}

				client.end(function (err) {
					if (err) {
						ctl.log.warn(err,
						    'pg disconnect');
					}
					client = null;
					xx();
				});
			};

			stch.update('verifying replication of sentinel');
			mod_vasync.waterfall([ function (cb) {
				if (ctl.pausing(cb)) {
					return;
				}

				var c = new mod_pg.Client({
					user: 'moray',
					password: 'moray',
					host: postgres_ip,
					database: 'moray',
					port: 5432,
				});

				ctl.log.info('connection to pg to verify');
				c.connect(function (err) {
					if (err) {
						cb(new VE(err, 'pg connect'));
						return;
					}

					ctl.log.info('pg connected!');
					client = c;
					cb();
				});

			}, function (cb) {
				if (ctl.pausing(cb)) {
					return;
				}

				var text = [
					'SELECT * FROM manta_reshard_sentinel',
				        'WHERE _key = \'sentinel\''
				].join(' ');

				client.query({ text: text },
				    function (err, res) {
					if (err) {
						cb(new VE(err, 'pg query'));
						return;
					}

					ctl.log.info({ rows: res.rows },
					    'results');

					if (res.rows.length !== 1) {
						cb(new VE('row count %d not 1',
						    res.rows.length));
						return;
					}

					if (res.rows[0]._etag !==
					    sentinel_etag) {
						cb(new VE('etag %s does not ' +
						    'match expected %s',
						    res.rows[0]._etag,
						    sentinel_etag));
						return;
					}

					var val = JSON.parse(
					    res.rows[0]._value);

					if (!mod_jsprim.deepEqual(val,
					    sentinel)) {
						cb(new VE('sentinel did not ' +
						    'match'));
						return;
					}

					ctl.log.info('sentinel OK!');
					cb();
				});

			} ], function (err) {
				close_client(function () {
					if (err) {
						if (VE.info(err).exe_pause) {
							/*
							 * This is a pause
							 * request, so don't
							 * retry forever here.
							 */
							done(err);
							return;
						}

						ctl.log.info(err, 'retry pg');
						setTimeout(check_for_sentinel,
						    5000);
						return;
					}

					stch.done();
					done();
				});
			});
		};

		check_for_sentinel();

	} ], function (err) {
		if (err) {
			callback(new VE(err, 'provisioning peer on %s',
			    server.uuid));
			return;
		}

		server.status.trunc();
		server.status.child().update('peer ok');
		callback();
	});
}

module.exports = {
	phase_create_manatee_peers: phase_create_manatee_peers,
};
