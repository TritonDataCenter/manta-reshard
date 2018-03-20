/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var lib_http_client = require('../lib/http_client');


if (process.argv.length !== 2) {
	console.error('ERROR: Usage: list');
	process.exit(1);
}

lib_http_client.http_get('127.0.0.1', 80, '/plans', function (err, res) {
	if (err) {
		lib_http_client.cmd_print_result(err, res);
		return;
	}

	console.log(Object.keys(res.plans).sort().join('\n'));
});
