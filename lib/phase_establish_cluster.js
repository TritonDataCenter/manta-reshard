
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_zkstream = require('zkstream');

var lib_manatee_adm = require('../lib/manatee_adm');
var lib_common = require('../lib/common');

var VE = mod_verror.VError;

var ROLES = [ 'primary', 'sync', 'async' ];

function
phase_establish_cluster(ctl)
{
	var p = ctl.plan();

	var path = '/manatee/' + p.new_shard + '/state';
	var cluster_state = lib_manatee_adm.generate_cluster_state({
		peers: ROLES.map(function (role) {
			return ({
				zone: ctl.prop_get('new_' + role),
				ip: ctl.prop_get('new_' + role + '_ip')
			});
		}),
		init_wal: ctl.prop_get('new_init_wal'),
		plan_uuid: p.uuid
	});

	var zk = null;
	var close_zk = function (callback) {
		if (zk === null) {
			setImmediate(callback);
			return;
		}

		zk.once('close', function () {
			zk = null;
			ctl.log.info('ZK connection closed');
			callback();
		});
		zk.close();
	};

	mod_vasync.waterfall([ function (done) {
		ctl.get_manta_app(done);

	}, function (app, done) {
		if (!app.metadata.ZK_SERVERS ||
		    app.metadata.ZK_SERVERS.length < 1) {
			done(new VE('ZK_SERVERS invalid'));
			return;
		}

		var opts = {
			address: app.metadata.ZK_SERVERS[0].host,
			port: app.metadata.ZK_SERVERS[0].port,
		};

		ctl.log.info({ zk_opts: opts }, 'connecting to ZK');
		var c = new mod_zkstream.Client(opts);

		c.on('failure', function () {
			/*
			 * XXX it's possible that we should give up and
			 * retry later when we get this event.  Note that
			 * the ZK client will keep trying forever, though.
			 */
			ctl.log.warn('ZK reports failure... (retrying)');
		});

		c.once('connect', function () {
			ctl.log.info('connected to ZK');
			zk = c;
			done();
		});

	}, function (done) {
		/*
		 * Attempt to write a cluster state object!
		 */
		ctl.log.info({ cluster_state: cluster_state }, 'writing ' +
		    'cluster state at "%s"', path);
		var data = new Buffer(JSON.stringify(cluster_state));
		zk.createWithEmptyParents(path, data, {},
		    function (err, newpath) {
			if (err) {
				if (err.code === 'NODE_EXISTS') {
					ctl.log.info('cluster state exists ' +
					    'already; skipping');
					done();
					return;
				}

				done(new VE(err, 'zk create "%s"', path));
				return;
			}

			ctl.log.info('created cluster state at "%s"', newpath);
			done();
		});

	}, function (done) {
		/*
		 * Wait for cluster to start up and settle.
		 */
		var primary = ctl.prop_get('new_primary');

		var wait_until_up = function () {
			lib_common.manatee_adm_show(ctl, primary,
			    function (err, cl) {
				if (err) {
					/*
					 * XXX
					 */
					ctl.log.info('retrying');
					setTimeout(wait_until_up, 5000);
					return;
				}

				if (cl.issues && cl.issues.length > 0) {
					ctl.log.info({ issues: cl.issues },
					    'Manatee reports issues');
					setTimeout(wait_until_up, 5000);
					return;
				}

				if (cl.props.cluster !== p.new_shard) {
					ctl.log.error({ cluster: cl },
					    'wrong shard?!');
					done(new VE('cluster came up ' +
					    'with wrong shard: %s',
					    cl.props.cluster));
					return;
				}

				if (cl.peers.length !== 3) {
					ctl.log.info('peers missing (retry)');
					setTimeout(wait_until_up, 5000);
					return;
				}

				var retry = false;

				cl.peers.forEach(function (peer, i) {
					var expect = ctl.prop_get('new_' +
					    ROLES[i]);
					if (peer.uuid !== expect) {
						ctl.log.info('peer[%d] had ' +
						    'wrong UUID %s', i,
						    peer.uuid);
						retry = true;
					}

					if (peer.role !== ROLES[i]) {
						ctl.log.info('peer[%d] had ' +
						    'wrong role "%s"', i,
						    peer.role);
						retry = true;
					}

					if (peer.pg !== 'ok') {
						ctl.log.info('peer[%d] pg ' +
						   'not ok (%s)', i, peer.pg);
						retry = true;
					}
				});

				if (retry) {
					ctl.log.info('peers missing (retry)');
					setTimeout(wait_until_up, 5000);
					return;
				}

				ctl.log.info({ cluster: cl }, 'cluster ok!');

				done();
			});
		};

		wait_until_up();

	} ], function (err) {
		close_zk(function () {
			if (err) {
				ctl.retry(err);
				return;
			}

			ctl.finish();
		});
	});
}

module.exports = {
	phase_establish_cluster: phase_establish_cluster,
};
