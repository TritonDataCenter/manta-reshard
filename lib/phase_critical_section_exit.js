/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_verror = require('verror');

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
