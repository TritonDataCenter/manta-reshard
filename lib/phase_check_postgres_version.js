
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var VE = mod_verror.VError;

var lib_manatee_adm = require('../lib/manatee_adm');

function
phase_check_postgres_version(ctl)
{
	var insts;
	var primary_uuid;
	var postgres_version;
	var plan = ctl.plan();
	var status = ctl.status();

	mod_vasync.waterfall([ function (done) {
		status.update('listing instances of "postgres"');

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
		 * Determine which peer is the primary for this cluster.
		 */
		status.update('locating primary Manatee peer via zone %s',
		    i.uuid);
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

			mod_assert.strictEqual(p.peers[0].role, 'primary',
			    'peer 0 should be primary');
			mod_assert.uuid(p.peers[0].uuid, 'primary uuid');

			primary_uuid = p.peers[0].uuid;

			done();
		});

	}, function (done) {
		status.update('checking PostgreSQL version via zone %s',
		    primary_uuid);
		ctl.log.info('checking PostgreSQL version in peer %s',
		    primary_uuid);

		ctl.zone_exec(primary_uuid,
		    'json -f /manatee/pg/manatee-config.json current',
		    function (err, res) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info({ result: res },
			    'Manatee PostgreSQL version');

			if (res.exit_status !== 0) {
				done(new VE('could not get PostgreSQL ' +
				    'version'));
				return;
			}

			postgres_version = res.stdout.trim();

			done();
		});

	} ], function (err) {
		if (err) {
			ctl.retry(err);
			return;
		}

		/*
		 * XXX For now, let's not be too push about the PostgreSQL
		 * version.  We should tighten this up if we do not end up
		 * testing on both 9.6 and 9.2 Manatee images.
		 */
		if (postgres_version === '9.6.3' ||
		    postgres_version === '9.2.4') {
			ctl.finish();
			return;
		}

		ctl.hold(new VE('PostgreSQL version was %s, wanted %s',
		    postgres_version, '9.6.3'));
	});
}

module.exports = {
	phase_check_postgres_version: phase_check_postgres_version,
};
