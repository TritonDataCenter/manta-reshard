
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;

function
phase_shutdown_new_peers(ctl)
{
	var p = ctl.plan();

	var insts;
	var primary;

	/*
	 * The rough plan:
	 *	For each peer we created earlier (i.e., the non-base peers),
	 *	starting at the _far_ end of the replication chain, we want to:
	 *		- select peer to shut down first and journal this
	 *		  into properties?
	 *		- shut down peer
	 *		- wait for peer to fall out of "manatee-adm show -v"
	 *		- adjust instance-level SAPI props to reflect new
	 *		  shard
	 *		- reprovision peer onto same image
	 */

	mod_vasync.waterfall([ function (done) {
		ctl.get_instances({ service: 'postgres', shard: p.shard },
		    function (err, _insts) {
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
		/*
		 * Determine which peer has the primary role.  Ensure that
		 * the cluster is still frozen.
		 */
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
		var candidates = insts.map(function (i) {
			return (i.uuid);
		});

		var roles = [ 'async', 'sync', 'primary' ];

		mod_vasync.forEachPipeline({ inputs: roles,
		    func: function (role, next) {
			shut_down_one_role(ctl, primary, candidates,
			    role, next);
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
shut_down_one_role(ctl, primary, candidates, role, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.uuid(primary, 'primary');
	mod_assert.arrayOfUuid(candidates, 'candidates');
	mod_assert.string(role, 'role');
	mod_assert.ok(role === 'primary' || role === 'sync' ||
	    role === 'async', 'valid role');
	mod_assert.func(callback, 'callback');

	var select = ctl.prop_get('new_' + role);

	mod_vasync.waterfall([ function (done) {
		if (select !== null) {
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
				    'is not one of our candidates!'));
				return;
			}

			ctl.log.info('selecting peer "%s" for new role "%s"',
			    new_peer.uuid);

			/*
			 * Journal our intent to use this peer for the nominated
			 * role, in case the resharding service restarts. XXX
			 */
			select = new_peer.uuid;
			ctl.prop_put('new_' + role, new_peer.uuid);
			ctl.prop_commit(done);
		});

	}, function (done) {
		/*
		 * XXX If this is the primary peer, we should probably take
		 * this (or an earlier) opportunity to record the appropriate
		 * "initWal" value for the new cluster state.
		 */

		/*
		 * Disable the Manatee Sitter for this zone.
		 */
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
		var wait_until_gone = function () {
			lib_common.manatee_adm_show(ctl, primary,
			    function (err, cl) {
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
