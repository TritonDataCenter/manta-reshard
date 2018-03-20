/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');
var lib_http_client = require('../lib/http_client');


function
usage()
{
	console.error('\tUsage: tune <plan_uuid> [<tune_name> ' +
	    '<tune_value>]');
	process.exit(1);
}


/*
 * If no tuning name and value are provided, just fetch all tuning values for
 * this plan.
 */
if (process.argv.length === 3 && lib_common.is_uuid(process.argv[2])) {
	lib_http_client.http_get('127.0.0.1', 80, '/plan/' + process.argv[2] +
	    '/tune', function (err, tune) {
		if (err) {
			console.error('ERROR: %s', err.message);
			process.exit(1);
		}

		console.log('%s', JSON.stringify(tune, null, 4));
	});
	return;
}

if (process.argv.length !== 5 || !lib_common.is_uuid(process.argv[2])) {
	console.error('ERROR: must provide all arguments');
	usage();
}

var tune_name = process.argv[3];
var tuning_value = null;

if (process.argv[4] !== 'null') {
	tuning_value = mod_jsprim.parseInteger(process.argv[4]);
	if (tuning_value instanceof Error) {
		console.error('ERROR: tune value must be a number, ' +
		    'or "null": %s', tuning_value.message);
		usage();
	}
}

lib_http_client.http_post('127.0.0.1', 80,
    '/plan/' + process.argv[2] + '/tune/' + tune_name,
    { tuning_value: tuning_value },
    lib_http_client.cmd_print_result);
