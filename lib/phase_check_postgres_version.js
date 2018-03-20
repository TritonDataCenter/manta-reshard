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

var lib_common = require('../lib/common');

var VE = mod_verror.VError;


function
phase_check_postgres_version(ctl)
{
	var insts;
	var postgres_version;
	var plan = ctl.plan();
	var status = ctl.status();

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

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
		if (ctl.pausing(done)) {
			return;
		}

		var i = insts[Object.keys(insts)[0]];

		/*
		 * Determine which peer is the primary for this cluster.
		 */
		status.update('locating primary Manatee peer');
		status.prop('via zone', i.uuid);
		lib_common.manatee_adm_show(ctl, i.uuid, done);

	}, function (p, done) {
		if (ctl.pausing(done)) {
			return;
		}

		mod_assert.strictEqual(p.peers[0].role, 'primary',
		    'peer 0 should be primary');
		mod_assert.uuid(p.peers[0].uuid, 'primary uuid');

		var primary_uuid = p.peers[0].uuid;

		status.clear();
		status.update('checking PostgreSQL version');
		status.prop('primary peer', primary_uuid);
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
		 * The resharding system has only been tested with Manatee
		 * peers running these PostgreSQL versions.
		 */
		var valid_versions = [ '9.6.3', '9.2.4' ];
		if (valid_versions.indexOf(postgres_version) !== -1) {
			ctl.finish();
			return;
		}

		ctl.hold(new VE('PostgreSQL version was %s, wanted one of %j',
		    postgres_version, valid_versions));
	});
}


module.exports = {
	phase_check_postgres_version: phase_check_postgres_version,
};
