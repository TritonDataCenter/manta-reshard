/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var VE = mod_verror.VError;


function
phase_update_sapi_mark_readwrite(ctl)
{
	var p = ctl.plan();
	var status = ctl.status();

	var hold = false;

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Update our view of the Manta application to make sure we
		 * have a current copy of INDEX_MORAY_SHARDS.
		 */
		status.update('refreshing "manta" SAPI application');
		ctl.get_manta_app(done);

	}, function (app, done) {
		if (ctl.pausing(done)) {
			return;
		}

		mod_assert.arrayOfObject(app.metadata.INDEX_MORAY_SHARDS,
		    'INDEX_MORAY_SHARDS');

		var found_new = false;
		var out = [];

		app.metadata.INDEX_MORAY_SHARDS.forEach(function (shard) {
			mod_assert.string(shard.host, 'shard.host');

			if (shard.host === p.new_shard) {
				found_new = true;
			}

			var copy = mod_jsprim.deepCopy(shard);
			delete copy.last;

			if (shard.host === p.shard ||
			    shard.host === p.new_shard) {
				delete copy.readOnly;
			}

			out.push(copy);
		});

		if (!found_new) {
			hold = true;
			done(new VE('new shard "%s" was not found in ' +
			    'INDEX_MORAY_SHARDS!', p.new_shard));
			return;
		}

		out[out.length - 1].last = true;

		var md = {
			INDEX_MORAY_SHARDS: out
		};

		status.update('updating "manta" SAPI application');
		ctl.log.info({ new_metadata: md }, 'updating SAPI application');
		ctl.update_app(app.uuid, { metadata: md }, function (err) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info('shard map updated');
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
	phase_update_sapi_mark_readwrite: phase_update_sapi_mark_readwrite,
};
