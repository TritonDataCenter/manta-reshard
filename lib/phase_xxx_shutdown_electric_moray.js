
var mod_path = require('path');
var mod_fs = require('fs');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_uuid = require('uuid');
var mod_jsprim = require('jsprim');

var VE = mod_verror.VError;

function
phase_xxx_shutdown_electric_moray(ctl)
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
		    'shutting down all Electric Moray instances');

		mod_vasync.forEachPipeline({ inputs: ilist,
		    func: function (uuid, next) {
			ctl.log.info('shutting down Electric Moray in zone ' +
			    '"%s"', uuid);
			ctl.zone_exec(uuid, 'svcadm disable -s ' +
			    '"*electric-moray*"', function (err, res) {
				if (err) {
					next(err);
					return;
				}

				if (res.exit_status !== 0) {
					next(new VE('disable service failed ' +
					    'in zone "%s": %s', uuid,
					    res.stderr.trim()));
					return;
				}

				ctl.log.info('ok');
				next();
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

module.exports = {
	phase_xxx_shutdown_electric_moray: phase_xxx_shutdown_electric_moray,
};
