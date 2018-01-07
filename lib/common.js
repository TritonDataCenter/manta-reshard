
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');
var mod_pg = require('pg');

var VE = mod_verror.VError;

var lib_manatee_adm = require('../lib/manatee_adm');

var REGEX_UUID = new RegExp('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-' +
    '[0-9a-f]{4}-[0-9a-f]{12}$');


function
is_uuid(uuid)
{
	if (uuid === undefined || uuid === null) {
		return (false);
	}

	mod_assert.string(uuid, 'uuid');

	return (!!uuid.match(REGEX_UUID));
}

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
		mod_assert.arrayOfString(p.issues, 'issues');

		callback(null, p);
	});
}

function
create_sentinel_object()
{
	return ({
		pid: process.pid,
		when: (new Date()).toISOString(),
		rand: Number(Math.round(Math.random() * 0xFFFFFFFF)).
		    toString(16)
	});
}

/*
 * Get the full list of instances of an SMF service like Electric Moray.
 * Note that the list of instances might not be stable until after zone
 * setup has completed.
 */
function
get_zone_smf_instances(ctl, zone, fmri, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.uuid(zone, 'zone');
	mod_assert.string(fmri, 'fmri');
	mod_assert.ok(fmri.match(/^svc:/), 'valid fmri');
	mod_assert.func(callback, 'callback');

	var fail = function (err) {
		callback(new VE(err, 'list instances of "%s" in "%s"',
		    fmri, zone));
	};

	ctl.log.info('listing instances of "%s" in zone "%s"', fmri, zone);
	ctl.zone_exec(zone, 'svcs -Ho fmri "' + fmri + '"',
	    function (err, res) {
		if (err) {
			fail(new VE('ur failure'));
			return;
		}

		if (res.exit_status !== 0) {
			fail(new VE('svcs failed: %s', res.stderr.trim()));
			return;
		}

		var list = res.stdout.trim().split('\n');

		for (var i = 0; i < list.length; i++) {
			/*
			 * Check that each line contains a valid SMF FMRI.
			 */
			if (!list[i].match(/^svc:/)) {
				fail(new VE('invalid FMRI found: "%s"',
				    list[i]));
				return;
			}
		}

		callback(null, list);
	});
}

function
get_zone_smf_propval(ctl, zone, fmri, propval, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.uuid(zone, 'zone');
	mod_assert.string(fmri, 'fmri');
	mod_assert.ok(fmri.match(/^svc:/), 'valid fmri');
	mod_assert.string(propval, 'propval');
	mod_assert.func(callback, 'callback');

	var fail = function (err) {
		callback(new VE(err, 'get property "%s" for "%s" in "%s"',
		    propval, fmri, zone));
	};

	ctl.log.info('fetch property "%s" for "%s" in zone "%s"', propval,
	    fmri, zone);
	ctl.zone_exec(zone, 'svcprop -p "' + propval + '" "' + fmri + '"',
	    function (err, res) {
		if (err) {
			fail(new VE('ur failure'));
			return;
		}

		if (res.exit_status !== 0) {
			fail(new VE('svcprop failed: %s', res.stderr.trim()));
			return;
		}

		callback(null, res.stdout.trim());
	});
}

function
check_zone_setup_complete(ctl, zone, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.uuid(zone, 'zone');
	mod_assert.func(callback, 'callback');

	/*
	 * The first-boot and every-boot scripts (setup.sh and configure.sh)
	 * are run by the user-script we provide when provisioning Manta zones.
	 * Right after a zone has been reprovisioned, this service might not
	 * yet have completed running these jobs.  In order to avoid an
	 * inconsistent view of per-instance configuration, we wait for it
	 * to be online.
	 */
	var fmri = 'svc:/smartdc/mdata:execute';

	check_zone_smf_online(ctl, zone, fmri, function (err) {
		if (err) {
			callback(new VE(err, 'check for zone setup in "%s"',
			    zone));
			return;
		}

		callback();
	});
}

function
check_zone_smf_online(ctl, zone, fmri, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.uuid(zone, 'zone');
	mod_assert.func(callback, 'callback');

	var fail = function (err) {
		callback(new VE(err, 'check status of "%s" in "%s"', fmri,
		    zone));
	};

	ctl.log.info('checking status of "%s" in "%s"', fmri, zone);
	ctl.zone_exec(zone, 'svcs -Ho sta "' + fmri + '"', function (err, res) {
		if (err) {
			fail(new VE('ur failure'));
			return;
		}

		if (res.exit_status !== 0) {
			fail(new VE('svcs failed: %s', res.stderr.trim()));
			return;
		}

		var out = res.stdout.trim();
		if (out !== 'ON') {
			var info = { smf_state: out };

			fail(new VE({ info: info }, 'status is "%s", ' +
			    'not online', out));
			return;
		}

		callback();
	});
}

module.exports = {
	manatee_adm_show: manatee_adm_show,
	create_sentinel_object: create_sentinel_object,
	check_zone_setup_complete: check_zone_setup_complete,
	get_zone_smf_instances: get_zone_smf_instances,
	get_zone_smf_propval: get_zone_smf_propval,
	check_zone_smf_online: check_zone_smf_online,
	is_uuid: is_uuid,
};
