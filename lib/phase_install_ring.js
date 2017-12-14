
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;

function
phase_install_ring(ctl)
{
	var p = ctl.plan();

	var hold = false;

	var old_ring = ctl.prop_get('old_hash_ring_uuid');
	mod_assert.uuid(old_ring, 'old_ring');

	var new_ring = ctl.prop_get('new_hash_ring_uuid');
	mod_assert.uuid(new_ring, 'new_ring');

	mod_vasync.waterfall([ function (done) {
		/*
		 * Update our view of the Manta application to make sure we
		 * have the current hash ring image UUID.
		 */
		ctl.get_manta_app(done);

	}, function (app, done) {
		mod_assert.uuid(app.metadata.HASH_RING_IMAGE,
		    'HASH_RING_IMAGE');

		if (app.metadata.HASH_RING_IMAGE === new_ring) {
			ctl.log.info('new ring installed already');
			setImmediate(done);
			return;
		}

		if (app.metadata.HASH_RING_IMAGE !== old_ring) {
			hold = true;
			done(new VE('HASH_RING_IMAGE is "%s", but our ' +
			    'new ring is based on "%s"',
			    app.metadata.HASH_RING_IMAGE, old_ring));
			return;
		}

		/*
		 * Update SAPI metadata to install new hash ring.
		 */
		var md = {
			HASH_RING_IMAGE: new_ring
		};

		ctl.log.info({ new_metadata: md }, 'updating SAPI application');

		ctl.update_app(app.uuid, { metadata: md }, function (err) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info('hash ring installed');
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
	phase_install_ring: phase_install_ring,
};