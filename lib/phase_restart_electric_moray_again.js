
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
phase_restart_electric_moray_again(ctl)
{
	var insts;
	var plan = ctl.plan();
	var scripts = {};

	mod_vasync.waterfall([ function (done) {
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
		var ilist = Object.keys(insts).sort();

		ctl.log.info({ instances: ilist },
		    'restarting all Electric Moray instances');

		mod_vasync.forEachPipeline({ inputs: ilist,
		    func: function (uuid, next) {
			ctl.log.info('restarting Electric Moray in zone ' +
			    '"%s"', uuid);
			lib_electric_moray.em_restart_one(ctl, uuid,
			    ensure_shards_are_read_write, next);

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
 * the existing shard and the new shard are present, and not marked read-only.
 * If either shard is missing, or marked read-only, return false to signify
 * that a configuration update is required.
 */
function
ensure_shards_are_read_write(ctl, lookup_shard)
{
	var p = ctl.plan();

	var s;
	var old_readwrite = false;
	if ((s = lookup_shard(p.shard)) !== null && !s.readOnly) {
		old_readwrite = true;
	}

	var new_readwrite = false;
	if ((s = lookup_shard(p.new_shard)) !== null && !s.readOnly) {
		new_readwrite = true;
	}

	return (old_readwrite && new_readwrite);
}

module.exports = {
	phase_restart_electric_moray_again: phase_restart_electric_moray_again,
};
