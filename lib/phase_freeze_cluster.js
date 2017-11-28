
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var VE = mod_verror.VError;

var lib_manatee_adm = require('../lib/manatee_adm');

function
phase_freeze_cluster(ctl)
{
	var insts;
	var primary_uuid;
	var postgres_version;
	var plan = ctl.plan();
	var is_frozen = false;

	mod_vasync.waterfall([ function (done) {
		ctl.get_instances({ service: 'postgres', shard: plan.shard },
		    function (err, _insts) {
			if (err) {
				done(err);
				return;
			}

			insts = _insts;
			done();
		});

	}, function (done) {
		var i = insts[Object.keys(insts)[0]];

		/*
		 * Check the cluster freeze status.
		 */
		ctl.log.info('running "manatee-adm show -v" in zone %s',
		    i.uuid);
		ctl.zone_exec(i.uuid, 'manatee-adm show -v',
		    function (err, res) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info({ result: res }, 'output');

			if (res.exit_status !== 0) {
				done(new VE('manatee-adm show failed'));
				return;
			}

			var p = lib_manatee_adm.parse_manatee_adm_show(
			    res.stdout);

			if (p instanceof Error) {
				done(p);
				return;
			}

			mod_assert.object(p.props, 'props');
			mod_assert.string(p.props.freeze, 'props.freeze');

			if (p.props.freeze.match(/^frozen since /)) {
				ctl.log.info('cluster frozen already: "%s"',
				    p.props.freeze_info || '?');
				is_frozen = true;
				done();
				return;
			}

			if (p.props.freeze !== 'not frozen') {
				done(new VE('unexpected freeze status: "%s"',
				    p.props.freeze));
				return;
			}

			is_frozen = false;
			done();
		});

	}, function (done) {
		if (is_frozen) {
			/*
			 * Cluster already frozen.  No need to freeze again.
			 */
			setImmediate(done);
			return;
		}

		var i = insts[Object.keys(insts)[0]];

		ctl.log.info('freezing cluster via zone %s', i.uuid);

		ctl.zone_exec(i.uuid, 'manatee-adm freeze -r ' +
		    '"reshard plan ' + ctl.plan().uuid + '"',
		    function (err, res) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info({ result: res }, 'output');

			if (res.exit_status !== 0) {
				done(new VE('manatee-adm freeze failed'));
				return;
			}

			ctl.log.info('cluster now frozen');
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
	phase_freeze_cluster: phase_freeze_cluster,
};
