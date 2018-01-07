
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;

function
phase_sapi_cleanup(ctl)
{
	var peers = [ 'primary', 'sync', 'async' ].map(function (role) {
		var uuid = ctl.prop_get('new_' + role);
		mod_assert.uuid(uuid, 'new_' + role + ' property');

		return ({ role: role, uuid: uuid });
	});

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Remove the tag which marks this Manatee peer as being a
		 * part of an active plan.
		 */
		ctl.log.info('cleaning SAPI instances');
		mod_vasync.forEachPipeline({ inputs: peers,
		    func: function (peer, next) {
			if (ctl.pausing(next)) {
				return;
			}

			ctl.log.info('cleaning SAPI instance "%s" (%s)',
			    peer.uuid, peer.role);
			cleanup_one(ctl, peer.uuid, function (err) {
				if (err) {
					next(new VE(err, 'cleaning zone "%s"',
					    peer.uuid));
					return;
				}

				next();
			});

		}}, function (err) {
			if (err) {
				done(new VE(err, 'updating SAPI instance'));
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

function
cleanup_one(ctl, zone, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.uuid(zone, 'zone');
	mod_assert.func(callback, 'callback');

	var p = ctl.plan();

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * It does not appear possible to delete a single tag.  Rather,
		 * we must read the full set of tags, remove the tag we don't
		 * want, then write the tag object back in full.
		 */
		ctl.get_instance(zone, done);

	}, function (inst, done) {
		if (ctl.pausing(done)) {
			return;
		}

		mod_assert.object(inst.params.tags, 'params.tags');

		if (!inst.params.tags.hasOwnProperty('manta_reshard_plan')) {
			/*
			 * The tag does not exist.  Note that we cannot assert
			 * that it _does_ exist, because we might have been
			 * restarted while removing it previously.
			 */
			ctl.log.info('"manta_reshard_plan" tag already ' +
			    'removed from zone "%s"', zone);
			setImmediate(done);
			return;
		}

		if (inst.params.tags.manta_reshard_plan !== p.uuid) {
			setImmediate(done, new VE({ info: { hold: true }},
			    'unexpected tag value: %j',
			    inst.params.tags.manta_reshard_plan));
			return;
		}

		var new_tags = mod_jsprim.deepCopy(inst.params.tags);
		delete new_tags.manta_reshard_plan;

		var update = {
			params: {
				tags: new_tags
			}
		};

		ctl.log.info({ update: update }, 'cleaning zone "%s"',
		    zone);

		ctl.update_instance(zone, update, done);

	} ], function (err) {
		callback(err);
	});
}

module.exports = {
	phase_sapi_cleanup: phase_sapi_cleanup,
};
