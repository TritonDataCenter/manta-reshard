
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var VE = mod_verror.VError;

var lib_manatee_adm = require('../lib/manatee_adm');

function
phase_create_sentinel_bucket(ctl)
{
	var plan = ctl.plan();

	mod_vasync.waterfall([ function (done) {
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

		ctl.log.info('creating "%s" bucket in shard "%s"', name,
		    plan.shard);
		ctl.target_moray().putBucket(name, bcfg, function (err) {
			if (err) {
				done(new VE(err, 'creating bucket "%s"', name));
				return;
			}

			ctl.log.info('bucket created');

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
	phase_create_sentinel_bucket: phase_create_sentinel_bucket,
};
