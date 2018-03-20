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

var VE = mod_verror.VError;

var lib_common = require('../lib/common');


function
phase_freeze_cluster(ctl)
{
	var insts;
	var plan = ctl.plan();
	var status = ctl.status();

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		status.update('listing instances of "postgres"');
		status.prop('shard', plan.shard);

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
		 * Check the cluster freeze status.
		 */
		status.clear();
		status.update('checking Manatee cluster freeze status');
		status.prop('via zone', i.uuid);
		lib_common.manatee_adm_show(ctl, i.uuid, done);

	}, function (p, done) {
		if (ctl.pausing(done)) {
			return;
		}

		mod_assert.object(p.props, 'props');
		mod_assert.string(p.props.freeze, 'props.freeze');

		if (p.props.freeze.match(/^frozen since /)) {
			ctl.log.info('cluster frozen already: "%s"',
			    p.props.freeze_info || '?');
			setImmediate(done);
			return;
		}

		if (p.props.freeze !== 'not frozen') {
			setImmediate(done, new VE(
			    'unexpected freeze status: "%s"',
			    p.props.freeze));
			return;
		}

		var i = insts[Object.keys(insts)[0]];

		status.clear();
		status.update('freezing Manatee cluster');
		status.prop('via zone', i.uuid);
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
