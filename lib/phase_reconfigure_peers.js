
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;

function
phase_reconfigure_peers(ctl)
{
	var p = ctl.plan();

	var peers = [ 'primary', 'sync', 'async' ].map(function (role) {
		return (ctl.prop_get('new_' + role));
	});
	mod_assert.arrayOfUuid(peers, 'peers');

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
		    func: function (uuid, next) {
			var update = {
				metadata: md,
				params: {
					alias: ctl.make_alias(uuid, 'postgres')
				}
			};

			ctl.log.info({ update: update }, 'updating "%s"',
			    uuid);
			ctl.update_instance(uuid, update, next);

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
		    func: function (uuid, next) {
			var alias = ctl.make_alias(uuid, 'postgres');

			ctl.log.info({ alias: alias }, 'updating "%s"',
			    uuid);
			ctl.set_vm_alias(uuid, alias, next);

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
		    func: function (uuid, next) {
			ctl.log.info('reprovisioning "%s"', uuid);
			ctl.reprovision_same_image(uuid, next);

		}}, function (err) {
			if (err) {
				done(new VE(err, 'reprovisioning'));
				return;
			}

			done();
		});

	} ], function (err) {
		if (err) {
			ctl.retry(err);
			return;
		}

		ctl.finish();
	});
}

module.exports = {
	phase_reconfigure_peers: phase_reconfigure_peers,
};
