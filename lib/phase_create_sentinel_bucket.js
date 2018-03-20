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

var VE = mod_verror.VError;


function
phase_create_sentinel_bucket(ctl)
{
	var plan = ctl.plan();
	var status = ctl.status();

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

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

		status.update('creating sentinel bucket "%s"', name);
		status.prop('shard', plan.shard);
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
