
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;

function
phase_critical_section_enter(ctl)
{
	var logged_owner = null;
	var status = ctl.status();

	status.update('waiting for critical section lock');

	var try_lock = function () {
		ctl.lock('critical_section', function (err, lock) {
			if (!err) {
				ctl.log.info('entered critical section');
				ctl.finish();
				return;
			}

			var info = VE.info(err);

			if (!info.lock_held) {
				/*
				 * Regular error; retry!
				 */
				ctl.retry(new VE(err, 'could not enter ' +
				    'critical section'));
				return;
			}

			/*
			 * Wait for lock to release.
			 */
			if (logged_owner !== info.owner) {
				status.trunc();
				status.child().update('lock held by "%s"',
				    info.owner);
				ctl.log.info('waiting on "%s" for ' +
				    'critical section lock', info.owner);
				logged_owner = info.owner;
			}
			setTimeout(try_lock, 15 * 1000);
		});
	};

	try_lock();
}

module.exports = {
	phase_critical_section_enter: phase_critical_section_enter,
};
