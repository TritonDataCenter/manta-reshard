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


/*
 * Shut down each of the Manatee peers we created in previous phases to prepare
 * them for reconfiguration as part of the new shard.
 *
 * Peers are shut down in reverse replication chain order; i.e., we first shut
 * down the async on the tail end of the chain.  This first peer will become
 * the async in the new shard.  After the async, the next peer will become the
 * sync, and then the final peer will become the new primary.
 */
function
phase_shutdown_new_peers(ctl)
{
	var p = ctl.plan();
	var status = ctl.status();

	var insts;
	var primary;

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		ctl.get_instances({ service: 'postgres', shard: p.shard },
		    status, function (err, _insts) {
			if (err) {
				done(err);
				return;
			}

			insts = Object.keys(_insts).map(function (k) {
				return (_insts[k]);
			});
			done();
		});

	}, function (done) {
		var errors = [];

		/*
		 * From the full set of instances for this shard, filter out
		 * only those we created for the new shard.
		 */
		insts = insts.filter(function (i) {
			mod_assert.object(i.params.tags, 'params.tags');

			var t = i.params.tags;

			if (!t.manta_reshard_plan) {
				return (false);
			}

			if (t.manta_reshard_plan !== p.uuid) {
				errors.push(new VE('instance "%s" for wrong ' +
				    'reshard plan (%s)',
				    t.manta_reshard_plan));
				return (false);
			}

			return (true);
		});

		if (insts.length !== 3) {
			errors.push(new VE('expected %d instances, found %d',
			    3, insts.length));
		}

		setImmediate(done, VE.errorFromList(errors));

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Determine which peer has the primary role.  Ensure that
		 * the cluster is still frozen.
		 */
		status.clear();
		status.update('locating primary Manatee peer');
		status.prop('via zone', insts[0].uuid);
		lib_common.manatee_adm_show(ctl, insts[0].uuid,
		    function (err, cluster) {
			if (err) {
				done(err);
				return;
			}

			mod_assert.strictEqual(cluster.peers[0].role,
			    'primary', 'peer 0 is primary');
			mod_assert.uuid(cluster.peers[0].uuid, 'primary uuid');
			primary = cluster.peers[0].uuid;

			ctl.log.info('peer %s is the primary!', primary);

			if (!cluster.props.freeze.match(/^frozen since /)) {
				done(new VE('cluster not frozen!'));
				return;
			}

			done();
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		var candidates = insts.map(function (i) {
			return (i.uuid);
		});

		var roles = [ 'async', 'sync', 'primary' ];

		status.clear();
		status.update('shutting down new Manatee peers');
		mod_vasync.forEachPipeline({ inputs: roles,
		    func: function (role, next) {
			if (ctl.pausing(next)) {
				return;
			}

			var stch = status.child();
			shut_down_one_role(ctl, primary, candidates,
			    role, stch, next);
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

function
shut_down_one_role(ctl, primary, candidates, role, status, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.uuid(primary, 'primary');
	mod_assert.arrayOfUuid(candidates, 'candidates');
	mod_assert.string(role, 'role');
	mod_assert.ok(role === 'primary' || role === 'sync' ||
	    role === 'async', 'valid role');
	mod_assert.func(callback, 'callback');

	var select = ctl.prop_get('new_' + role);
	var stch;
	var postgres_ip;
	var sentinel, sentinel_etag;
	var resumed = false;

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		if (select !== null) {
			/*
			 * A peer was selected for this role previously, but
			 * our execution was interrupted.  Pick up where we
			 * left off.
			 */
			resumed = true;
			postgres_ip = ctl.prop_get('new_' + role + '_ip');
			ctl.log.info('already selected peer "%s" (ip "%s") ' +
			    'for role "%s"', select, postgres_ip, role);

			mod_assert.uuid(select, 'select');
			mod_assert.ok(mod_net.isIPv4(postgres_ip),
			    'postgres_ip');

			setImmediate(done);
			return;
		}

		/*
		 * If we have not yet selected a peer for this role, do so
		 * now.
		 */
		var expect_count = (role === 'async') ? 6 :
		    (role === 'sync') ? 5 :
		    (role === 'primary') ? 4 : -1;

		status.update('role %s: selecting zone...', role, select);
		lib_common.manatee_adm_show(ctl, primary, function (err, cl) {
			if (err) {
				done(err);
				return;
			}

			if (cl.peers.length !== expect_count) {
				done(new VE('while picking "%s" peer, ' +
				    'expected %d remaining peers; found %d',
				    role, expect_count, cl.peers.length));
				return;
			}

			/*
			 * Choose the last peer in the chain for this role.
			 */
			var new_peer = cl.peers[cl.peers.length - 1];
			if (new_peer.role !== 'async' || new_peer.pg !== 'ok') {
				done(new VE('last peer in chain "%s" was not ' +
				    'a healthy async', new_peer.uuid));
				return;
			}

			/*
			 * Make sure the last peer is one of our candidates.
			 */
			if (candidates.indexOf(new_peer.uuid) === -1) {
				done(new VE('peer "%s", last in chain, ' +
				    'is not one of our candidates!',
				    new_peer.uuid));
				return;
			}

			ctl.log.info('selecting peer "%s" for new role "%s"',
			    new_peer.uuid, role);

			mod_assert.uuid(new_peer.uuid, 'new_peer.uuid');
			mod_assert.ok(mod_net.isIPv4(new_peer.ip,
			    'new_peer.ip'));

			select = new_peer.uuid;
			postgres_ip = new_peer.ip;

			done();
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		status.update('role %s: zone %s', role, select);
		stch = status.child();

		if (role !== 'primary') {
			setImmediate(done);
			return;
		}

		/*
		 * XXX If this is the primary peer, we should probably take
		 * this opportunity to record the appropriate
		 * "initWal" value for the new cluster state.
		 *
		 * For now, we'll just store a fake value.
		 */
		ctl.prop_put('new_init_wal', '0/00000000');
		ctl.prop_commit(done);

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		if (resumed) {
			setImmediate(done);
			return;
		}

		/*
		 * We need to flush the sentinel object through the replication
		 * chain again to ensure the replicas are up-to-date now
		 * that we have marked the shard read-only.
		 */
		stch.update('updating sentinel object');

		sentinel = lib_common.create_sentinel_object();

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
		if (ctl.pausing(done)) {
			return;
		}

		if (resumed) {
			setImmediate(done);
			return;
		}

		stch.update('verifying replication of sentinel');
		lib_common.check_for_sentinel(ctl, { sentinel: sentinel,
		    sentinel_etag: sentinel_etag, postgres_ip: postgres_ip },
		    done);

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		if (resumed) {
			setImmediate(done);
			return;
		}

		/*
		 * Journal our intent to use this peer for the nominated role,
		 * in case the resharding service restarts.  This must be done
		 * _after_ the replication check, so that if we are restarted
		 * part way through we do not try to check replication against
		 * a disabled Manatee sitter.
		 */
		mod_assert.uuid(select, 'select');
		mod_assert.ok(mod_net.isIPv4(postgres_ip), 'postgres_ip');
		ctl.prop_put('new_' + role, select);
		ctl.prop_put('new_' + role + '_ip', postgres_ip);
		ctl.prop_commit(done);

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Disable the Manatee Sitter for this zone.
		 */
		stch.update('disable Manatee sitter');
		ctl.log.info('disable Manatee sitter in zone %s', select);
		ctl.zone_exec(select, 'svcadm disable manatee-sitter',
		    function (err, res) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info({ result: res }, 'result');

			if (res.exit_status !== 0) {
				/*
				 * XXX
				 */
				done(new VE('failed to disable sitter'));
				return;
			}

			done();
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		stch.update('waiting for peer to leave cluster');
		var wait_until_gone = function () {
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
					ctl.log.info(err, 'retrying');
					setTimeout(wait_until_gone, 5000);
					return;
				}

				var chk = cl.peers.filter(function (peer) {
					return (peer.uuid === select);
				});

				if (chk.length !== 0) {
					ctl.log.info('waiting for peer to ' +
					    'disappear');
					setTimeout(wait_until_gone, 5000);
					return;
				}

				stch.update('peer correctly offline');
				ctl.log.info('peer disappeared!');
				done();
			});
		};

		wait_until_gone();

	} ], function (err) {
		callback(err);
	});
}


module.exports = {
	phase_shutdown_new_peers: phase_shutdown_new_peers,
};
