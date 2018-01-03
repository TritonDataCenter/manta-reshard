
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;

function
phase_update_sapi(ctl)
{
	var p = ctl.plan();
	var status = ctl.status();

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
				copy.readOnly = true;
			}

			out.push(copy);
		});

		if (found_new) {
			ctl.log.info('new shard "%s" already in ' +
			    'INDEX_MORAY_SHARDS', p.new_shard);
		} else {
			ctl.log.info('adding new shard "%s" to ' +
			    'INDEX_MORAY_SHARDS', p.new_shard);
			out.push({ host: p.new_shard, readOnly: true });
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
			ctl.retry(err);
			return;
		}

		ctl.finish();
	});
}

module.exports = {
	phase_update_sapi: phase_update_sapi,
};
