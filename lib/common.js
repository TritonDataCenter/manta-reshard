
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');
var mod_pg = require('pg');

var VE = mod_verror.VError;

var lib_manatee_adm = require('../lib/manatee_adm');


function
manatee_adm_show(ctl, zone, callback)
{
	ctl.log.info('running "manatee_adm show -v" in zone %s', zone);
	ctl.zone_exec(zone, 'manatee-adm show -v', function (err, res) {
		if (err) {
			callback(err);
			return;
		}

		if (res.exit_status !== 0) {
			callback(new VE('manatee-adm show failed'));
			return;
		}

		var p = lib_manatee_adm.parse_manatee_adm_show(res.stdout);

		if (p instanceof Error) {
			callback(p);
			return;
		}

		mod_assert.array(p.peers, 'peers');
		mod_assert.object(p.props, 'props');

		callback(null, p);
	});
}

module.exports = {
	manatee_adm_show: manatee_adm_show,
};
