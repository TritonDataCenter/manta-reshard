
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;

function
phase_critical_section_exit(ctl)
{
	ctl.unlock('critical_section', function (err) {
		if (err) {
			ctl.retry(new VE(err,
			    'failed to exit critical section'));
			return;
		}

		ctl.log.info('exited critical section');
		ctl.finish();
	});
}

module.exports = {
	phase_critical_section_exit: phase_critical_section_exit,
};
