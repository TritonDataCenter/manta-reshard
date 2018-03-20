/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_zkstream = require('zkstream');

var lib_manatee_adm = require('../lib/manatee_adm');
var lib_common = require('../lib/common');

var VE = mod_verror.VError;


/*
 * When we establish a new Manatee cluster, we need to nominate a peer for each
 * of these three roles.
 */
var ROLES = [ 'primary', 'sync', 'async' ];


function
phase_establish_cluster(ctl)
{
	var p = ctl.plan();
	var status = ctl.status();

	status.update('establishing new Manatee cluster');
	status.prop('shard', p.new_shard);

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

	var stch = status.child();
	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		stch.update('refreshing "manta" SAPI application');
		ctl.get_manta_app(done);

	}, function (app, done) {
		if (ctl.pausing(done)) {
			return;
		}

		if (!app.metadata.ZK_SERVERS ||
		    app.metadata.ZK_SERVERS.length < 1) {
			done(new VE('ZK_SERVERS invalid'));
			return;
		}

		var opts = {
			address: app.metadata.ZK_SERVERS[0].host,
			port: app.metadata.ZK_SERVERS[0].port,
		};

		stch.update('connecting to zookeeper');
		stch.prop('ip', app.metadata.ZK_SERVERS[0].host);
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
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Attempt to write a cluster state object!
		 */
		stch.update('writing cluster state to zookeeper');
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
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Wait for cluster to start up and settle.
		 */
		var primary = ctl.prop_get('new_primary');

		stch.clear();
		stch.update('waiting for cluster to come online');
		var wait_until_up = function () {
			if (ctl.pausing(done)) {
				return;
			}

			lib_common.manatee_adm_show(ctl, primary,
			    function (err, cl) {
				if (ctl.pausing(done)) {
					return;
				}

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

				stch.update('cluster ok');
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
