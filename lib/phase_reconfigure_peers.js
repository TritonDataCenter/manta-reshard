
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;

function
phase_reconfigure_peers(ctl)
{
	var p = ctl.plan();
	var hold = false;

	var peers = [ 'primary', 'sync', 'async' ].map(function (role) {
		var uuid = ctl.prop_get('new_' + role);
		mod_assert.uuid(uuid, 'new_' + role + ' property');

		var st = ctl.status();
		st.update('role %s: zone %s', role, uuid);

		return ({ role: role, uuid: uuid, status: st });
	});

	mod_vasync.waterfall([ function (done) {
		/*
		 * Update SAPI metadata to "move" the selected peers into the
		 * new shard.
		 */
		var md = {
			SERVICE_NAME: p.new_shard,
			SHARD: ctl.new_short_shard(),
			MANATEE_SHARD_PATH: '/manatee/' + p.new_shard,
		};

		ctl.log.info({ new_metadata: md }, 'updating SAPI instances');

		mod_vasync.forEachPipeline({ inputs: peers,
		    func: function (peer, next) {
			var update = {
				metadata: md,
				params: {
					alias: ctl.make_alias(peer.uuid,
					    'postgres')
				}
			};

			peer.status.child().update('updating SAPI instance');
			ctl.log.info({ update: update }, 'updating "%s"',
			    peer.uuid);
			ctl.update_instance(peer.uuid, update, function (err) {
				peer.status.trunc();
				next(err);
			});

		}}, function (err) {
			if (err) {
				done(new VE(err, 'updating SAPI instance'));
				return;
			}

			done();
		});

	}, function (done) {
		ctl.log.info('updating VMAPI aliases');

		/*
		 * Update each instance alias via VMAPI.
		 */
		mod_vasync.forEachPipeline({ inputs: peers,
		    func: function (peer, next) {
			var alias = ctl.make_alias(peer.uuid, 'postgres');

			peer.status.child().update('updating VM alias');
			ctl.log.info({ alias: alias }, 'updating "%s"',
			    peer.uuid);
			ctl.set_vm_alias(peer.uuid, alias, function (err) {
				peer.status.trunc();
				next(err);
			});

		}}, function (err) {
			if (err) {
				done(new VE(err, 'updating alias'));
				return;
			}

			done();
		});

	}, function (done) {
		ctl.log.info('reprovisioning VMs');

		/*
		 * Reprovision each instance via SAPI.  This will remove any
		 * latent logs or configuration files for the original shard
		 * and cause the instance to come up "clean" with the
		 * configuration for the new shard.
		 */
		mod_vasync.forEachPipeline({ inputs: peers,
		    func: function (peer, next) {
			var previous = ctl.prop_get('new_' + peer.role +
			    '_reprovision');
			if (previous !== null) {
				ctl.log.info('skipping reprovision of "%s" ' +
				    'due to previous reprovision (%s)',
				    peer.uuid, previous);
				next();
				return;
			}

			peer.status.child().update('reprovisioning');
			ctl.log.info('reprovisioning "%s"', peer.uuid);
			ctl.reprovision_same_image(peer.uuid, function (err) {
				peer.status.trunc();

				if (err) {
					/*
					 * Failed reprovisions will almost
					 * certainly require operator
					 * intervention.
					 */
					hold = true;

					next(err);
					return;
				}

				/*
				 * Record the fact that we have managed to
				 * reprovision this zone in case we are
				 * restarted.  The reprovision operation can
				 * be unreliable, so we avoid doing it more
				 * than is strictly necessary.
				 */
				peer.status.child().update('complete');
				ctl.prop_put('new_' + peer.role +
				    '_reprovision', (new Date()).toISOString());
				ctl.prop_commit(next);
			});

		}}, function (err) {
			if (err) {
				done(new VE(err, 'reprovisioning'));
				return;
			}

			done();
		});

	} ], function (err) {
		if (err) {
			if (hold) {
				ctl.hold(err);
			} else {
				ctl.retry(err);
			}
			return;
		}

		ctl.finish();
	});
}

module.exports = {
	phase_reconfigure_peers: phase_reconfigure_peers,
};
