
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var VE = mod_verror.VError;

var lib_manatee_adm = require('../lib/manatee_adm');
var lib_common = require('../lib/common');

function
phase_unfreeze_cluster(ctl)
{
	var plan = ctl.plan();
	var status = ctl.status();

	status.update('unfreezing both Manatee shards');

	mod_vasync.forEachPipeline({ inputs: [ plan.shard, plan.new_shard ],
	    func: function (shard, next) {
		if (ctl.pausing(next)) {
			return;
		}

		var stch = status.child();
		stch.update('shard %s', shard);
		unfreeze_shard(ctl, shard, stch.child(), next);

	}}, function (err) {
		if (err) {
			ctl.retry(err);
			return;
		}

		ctl.finish();
	});
}

function
unfreeze_shard(ctl, shard, status, callback)
{
	var zone;

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		status.update('listing instances of "postgres"');
		status.prop('shard', shard);

		ctl.get_instances({ service: 'postgres', shard: shard }, done);

	}, function (insts, done) {
		if (ctl.pausing(done)) {
			return;
		}

		zone = insts[Object.keys(insts)[0]].uuid;

		/*
		 * Check the cluster freeze status.
		 */
		status.clear();
		status.update('checking Manatee cluster freeze status');
		status.prop('via zone', zone);
		lib_common.manatee_adm_show(ctl, zone, done);

	}, function (p, done) {
		if (ctl.pausing(done)) {
			return;
		}

		mod_assert.object(p.props, 'props');
		mod_assert.string(p.props.freeze, 'props.freeze');

		if (p.props.freeze.match(/^frozen since /)) {
			ctl.log.info('cluster still frozen: "%s"',
			    p.props.freeze_info || '?');

			if (p.props.freeze_info !== 'reshard plan ' +
			    ctl.plan().uuid) {
				status.clear();
				status.update('unexpected freeze info; ' +
				    'leaving frozen');
				status.prop('freeze info',
				    p.props.freeze_info);
				ctl.log.warn('unexpected freeze info: "%s"; ' +
				    'not unfreezing!', p.props.freeze_info);
				setImmediate(done);
				return;
			}

		} else if (p.props.freeze !== 'not frozen') {
			setImmediate(done, new VE(
			    'unexpected freeze status: "%s"',
			    p.props.freeze));
			return;

		} else {
			status.clear();
			status.update('cluster already unfrozen');
			ctl.log.info('cluster already unfrozen');
			setImmediate(done);
			return;
		}

		status.clear();
		status.update('unfreezing Manatee cluster');
		status.prop('via zone', zone);
		ctl.log.info('unfreezing cluster via zone %s', zone);

		/*
		 * XXX It would be great if there was a "manatee-adm unfreeze"
		 * variant which accepted the expected freeze reason, and would
		 * refuse to unfreeze if the string did not match.
		 */
		ctl.zone_exec(zone, 'manatee-adm unfreeze',
		    function (err, res) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info({ result: res }, 'output');

			if (res.exit_status !== 0) {
				done(new VE('manatee-adm unfreeze failed'));
				return;
			}

			status.clear();
			status.update('unfrozen ok');
			ctl.log.info('cluster now unfrozen');
			done();
		});

	} ], function (err) {
		if (err) {
			callback(new VE('unfreezing shard "%s"', shard));
			return;
		}

		callback();
	});
}

module.exports = {
	phase_unfreeze_cluster: phase_unfreeze_cluster,
};
