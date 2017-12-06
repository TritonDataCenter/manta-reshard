
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');
var mod_pg = require('pg');

var VE = mod_verror.VError;

var lib_common = require('../lib/common');


function
phase_create_morays(ctl)
{
	var p = ctl.plan();

	var image = null;
	var servers = mod_jsprim.deepCopy(p.servers);
	servers.forEach(function (s) {
		s.moray_zone = null;
	});
	var sentinel, sentinel_etag;

	/*
	 * XXX Until we sort out exactly how we want to do this, we'll just put
	 * a single Moray instance on the first server in the list.
	 *
	 * XXX We'll use the first image we see in an existing Moray instance
	 * for the source shard.
	 */

	mod_vasync.waterfall([ function (done) {
		/*
		 * Collect the list of Moray instances for the target shard.
		 */
		ctl.get_instances({ service: 'moray', shard: p.shard },
		    function (err, _insts) {
			if (err) {
				done(err);
				return;
			}

			var uuids = Object.keys(_insts);
			if (uuids.length < 1) {
				/*
				 * XXX
				 */
				done(new VE('no Moray instances found for ' +
				    'existing shard "%s"', p.shard));
				return;
			}

			image = _insts[uuids[0]].params.image_uuid;
			mod_assert.uuid(image, 'image');

			done();
		});
	}, function (done) {
		/*
		 * Collect the list of Moray instances for the new shard and
		 * associate them with the servers in the deployment plan.
		 */
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
		/*
		 * Check each server in the plan.  If the server does not have
		 * a Manatee peer, we need to provision one.
		 * XXX Note: just using the first server in the plan for now.
		 */
		mod_vasync.forEachPipeline({ inputs: servers.slice(0, 1),
		    func: function (server, next) {
			create_one_moray(ctl, {
				server: server,
				image: image,
			}, next);

		}}, function (err) {
			done(err);
		});

	}, function (done) {
		/*
		 * Get a Moray client for the new shard.
		 */
		ctl.new_shard_moray(done);

	}, function (client, done) {
		/*
		 * Update the sentinel object in the new shard.
		 */
		sentinel = lib_common.create_sentinel_object();

		ctl.log.info({ sentinel: sentinel }, 'sentinel object');

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
		/*
		 * Use the PostgreSQL IP for the async peer to check for
		 * replication of the sentinel update.
		 */
		var postgres_ip = ctl.prop_get('new_async_ip');

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

	} ], function (err) {
		if (err) {
			ctl.retry(err);
			return;
		}

		ctl.hold(new VE('holding!'));
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
		if (server.moray_zone !== null) {
			ctl.log.info('server %s in %s already has ' +
			    'peer %s', server.uuid, server.dc_name,
			    server.moray_zone);
			done();
			return;
		}

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
