/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_verror = require('verror');
var mod_extsprintf = require('extsprintf');

var lib_http_client = require('../lib/http_client');

var VE = mod_verror.VError;


lib_http_client.http_get('127.0.0.1', 80, '/phases', function (err, res) {
	if (err) {
		console.error('ERROR: %s', VE.fullStack(err));
		process.exit(1);
	}

	if (!res.phases) {
		console.error('RESULT DID NOT INCLUDE "phases"');
		process.exit(1);
	}

	console.log('PHASES:\n');

	for (var i = 0; i < res.phases.length; i++) {
		var o = mod_extsprintf.sprintf('    %2d.  %s', i + 1,
		    res.phases[i]);

		console.log(o);
	}

	console.log('');
});
