
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');
var mod_pg = require('pg');

var VE = mod_verror.VError;

var lib_manatee_adm = require('../lib/manatee_adm');


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
		if (server.manatee_peer !== null) {
			ctl.log.info('server %s in %s already has ' +
			    'peer %s', server.uuid, server.dc_name,
			    server.manatee_peer);
			done();
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
				done(new VE(err, 'creating instance'));
				return;
			}

			ctl.log.info({ instance: inst },
			    'created instance "%s" in DC %s',
			    inst.uuid, server.dc_name);

			mod_assert.strictEqual(server.manatee_peer, null,
			    'manatee_peer set twice?');
			server.manatee_peer = inst.uuid;

			done();
		});

	}, function (done) {
		/*
		 * Update the sentinel object.
		 */
		sentinel = {
			pid: process.pid,
			when: (new Date()).toISOString(),
			rand: Number(Math.round(Math.random() * 0xFFFFFFFF)).
			    toString(16)
		};

		ctl.log.info({ sentinel: sentinel }, 'sentinel object');

		ctl.target_moray().putObject('manta_reshard_sentinel',
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
		/*
		 * Wait for the new instance to be visible in the Manatee
		 * cluster as an "async" peer.
		 */
		var zuuid = base_peers[0];

		var check_for_peer = function () {
			manatee_adm_show(ctl, base_peers[0], function (err,
			    show) {
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

				ctl.log.info({ peer: zp[0] }, 'peer OK!');
				done();
			});
		};

		check_for_peer();

	}, function (done) {
		/*
		 * Now that we have seen the peer correctly in the cluster,
		 * attempt to read our sentinel value from it.  We need to do
		 * this using a raw PostgreSQL connection, as Moray will only
		 * ever read from the primary peer in the cluster.
		 */
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

			mod_vasync.waterfall([ function (cb) {
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
						ctl.log.info(err, 'retry pg');
						setTimeout(check_for_sentinel,
						    5000);
						return;
					}

					done();
				});
			});
		};

		check_for_sentinel();

	/*}, function (done) {
		ctl.log.warn('WAITING FOREVER');
		var wait_forever = function () {
			setTimeout(wait_forever, 1000);
		};

		wait_forever();*/

	} ], function (err) {
		if (err) {
			callback(new VE(err, 'provisioning peer on %s',
			    server.uuid));
			return;
		}

		callback();
	});
}

function
manatee_adm_show(ctl, zone, callback)
{
	ctl.log.info('running "manatee_adm show -v" in zone %s', zone);
	ctl.zone_exec(zone, 'manatee-adm show -v', function (err, res) {
		if (err) {
			callback(err);
			return;
		}

		if (res.exit_status !== 0) {
			callback(new VE('manatee-adm show failed'));
			return;
		}

		var p = lib_manatee_adm.parse_manatee_adm_show(res.stdout);

		if (p instanceof Error) {
			callback(p);
			return;
		}

		mod_assert.array(p.peers, 'peers');
		mod_assert.object(p.props, 'props');

		callback(null, p);
	});
}

module.exports = {
	phase_create_manatee_peers: phase_create_manatee_peers,
};
