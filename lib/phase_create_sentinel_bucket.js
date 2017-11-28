
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var VE = mod_verror.VError;

var lib_manatee_adm = require('../lib/manatee_adm');

function
phase_create_sentinel_bucket(ctl)
{
	var insts;
	var primary_uuid;
	var postgres_version;
	var plan = ctl.plan();
	var is_frozen = false;

	var moray_client;
	var close_moray_client = function (callback) {
		if (!moray_client) {
			setImmediate(callback);
			return;
		}

		moray_client.once('close', function () {
			moray_client = null;
			ctl.log.info('moray client closed');
			callback();
		});

		ctl.log.info('closing moray client');
		moray_client.close();
	};

	mod_vasync.waterfall([ function (done) {
		/*
		 * Create a Moray client for the target shard.  We will use
		 * this to create a sentinel bucket.  The purpose of this
		 * bucket is to be able to update a sentinel object on the
		 * primary peer, and watch for the update to flow through
		 * to the far end of the Manatee replication chain.
		 */
		ctl.moray_client(plan.shard, function (err, mc) {
			if (err) {
				done(err);
				return;
			}

			moray_client = mc;
			done();
		});

	}, function (done) {
		/*
		 * The sentinel bucket does not need any indexes.  We will
		 * write a single object, named "sentinel", with a few
		 * JSON properties that allow us to verify that updates flow
		 * through the replication chain.
		 */
		var name = 'manta_reshard_sentinel';
		var bcfg = {
			options: {
				version: 1
			}
		};

		ctl.log.info('creating "%s" bucket in shard "%s"',
		    name, plan.shard);
		moray_client.putBucket(name, bcfg, function (err) {
			if (err) {
				done(new VE(err, 'creating bucket "%s"', name));
				return;
			}

			ctl.log.info('bucket created');

			done();
		});

	} ], function (err) {
		close_moray_client(function () {
			if (err) {
				ctl.retry(err);
				return;
			}

			ctl.finish();
		});
	});
}

module.exports = {
	phase_create_sentinel_bucket: phase_create_sentinel_bucket,
};
