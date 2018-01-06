
var mod_path = require('path');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_net = require('net');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_uuid = require('uuid');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');
var lib_electric_moray = require('../lib/electric_moray');

var VE = mod_verror.VError;

function
phase_restart_electric_moray_readonly(ctl)
{
	var insts;
	var plan = ctl.plan();
	var status = ctl.status();
	var scripts = {};

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		ctl.get_instances({ service: 'electric-moray' },
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

		var ilist = Object.keys(insts).sort();
		var cnt = 0;

		status.update('restarting all %d Electric Moray instances',
		    ilist.length);
		ctl.log.info({ instances: ilist },
		    'restarting all Electric Moray instances');

		mod_vasync.forEachPipeline({ inputs: ilist,
		    func: function (uuid, next) {
			if (ctl.pausing(next)) {
				return;
			}

			cnt++;
			var stch = status.child();
			stch.update('zone %s (%d/%d)', uuid, cnt, ilist.length);
			ctl.log.info('restarting Electric Moray in zone ' +
			    '"%s"', uuid);
			lib_electric_moray.em_restart_one(ctl, uuid,
			    ensure_shards_are_read_only, stch, function (err) {
				stch.trunc();
				if (err) {
					stch.child().update('failed: %s',
					    err.message);
				} else {
					stch.child().update('ok');
				}
				next(err);
			});

		}}, function (err) {
			done(err);
		});

	} ], function (err) {
		if (err) {
			ctl.retry(err);
			return;
		}

		ctl.finish();
	});
}

/*
 * Check the index map for this Electric Moray instance, verifying that both
 * the existing shard and the new shard are present, and are marked read-only.
 * If either shard is missing, or not marked read-only, return false to signify
 * that a configuration update is required.
 */
function
ensure_shards_are_read_only(ctl, lookup_shard)
{
	var p = ctl.plan();

	var s;
	var old_readonly = false;
	if ((s = lookup_shard(p.shard)) !== null && s.readOnly) {
		old_readonly = true;
	}

	var new_readonly = false;
	if ((s = lookup_shard(p.new_shard)) !== null && s.readOnly) {
		new_readonly = true;
	}

	return (old_readonly && new_readonly);
}

module.exports = {
	phase_restart_electric_moray_readonly:
	    phase_restart_electric_moray_readonly,
};
