/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_verror = require('verror');
var mod_getopt = require('posix-getopt');

var lib_common = require('../lib/common');
var lib_status = require('../lib/status');
var lib_http_client = require('../lib/http_client');

var VE = mod_verror.VError;


var PLAN_MATCH_LIST = null;
var ONLY_ERRORS = false;
var REDRAW = false;
var FORCE_UTF8 = false;


function
include_plan(plan_uuid)
{
	if (PLAN_MATCH_LIST === null) {
		return (true);
	}

	for (var i = 0; i < PLAN_MATCH_LIST.length; i++) {
		var pme = PLAN_MATCH_LIST[i];

		if (pme.test(plan_uuid)) {
			return (true);
		}
	}

	return (false);
}

function
print_status(callback)
{
	lib_http_client.http_get('127.0.0.1', 80, '/plans',
	    function (err, res) {
		if (REDRAW) {
			console.log('\u001b[H\u001b[2JDATE: %s\n',
			    (new Date()).toISOString());
		}

		if (err) {
			callback(err);
			return;
		}

		if (res.problems.length > 0) {
			console.log('PROBLEMS:');
			res.problems.forEach(function (problem) {
				console.log('  » %s', problem);
			});
			console.log('');
		}

		if (!res.plans) {
			callback(new VE('RESULT DID NOT INCLUDE "plans"'));
			return;
		}

		var k = Object.keys(res.plans).sort(function (ka, kb) {
			var pa = res.plans[ka];
			var pb = res.plans[kb];

			if (pa.shard !== pb.shard) {
				var na = parseInt(pa.shard, 10);
				var nb = parseInt(pb.shard, 10);

				if (!isNaN(na) && !isNaN(nb)) {
					return (na - nb);
				}
			}

			return (ka > kb ? 1 : ka < kb ? -1 : 0);
		});

		for (var i = 0; i < k.length; i++) {
			var p = res.plans[k[i]];

			if (ONLY_ERRORS) {
				if (!p.held && !p.retrying) {
					continue;
				}
			}

			if (!include_plan(k[i])) {
				continue;
			}

			lib_status.pretty_print(p.status,
			    { force_utf8: FORCE_UTF8 });

			console.log('');
		}

		callback();
	});
}

function
redraw_callback(err)
{
	if (err) {
		console.log('ERROR: %s', VE.fullStack(err));
	}
	setTimeout(print_status, 500, redraw_callback);
}


var option;
var parser = new mod_getopt.BasicParser('rxU', process.argv);

while ((option = parser.getopt()) !== undefined) {
	switch (option.option) {
	case 'r':
		REDRAW = true;
		break;

	case 'x':
		ONLY_ERRORS = true;
		break;

	case 'U':
		FORCE_UTF8 = true;
		break;

	default:
		console.error('usage: status [-r] [-x] [-U] [PLAN_UUID ...]');
		process.exit(1);
		break;
	}
}

/*
 * Turn positional arguments into plan UUID filters.
 */
process.argv.slice(parser.optind()).forEach(function (a) {
	if (PLAN_MATCH_LIST === null) {
		PLAN_MATCH_LIST = [];
	}

	if (lib_common.is_uuid(a)) {
		PLAN_MATCH_LIST.push(new RegExp('^' + a + '$'));
	} else {
		PLAN_MATCH_LIST.push(new RegExp(a));
	}
});

print_status(REDRAW ? redraw_callback : function (err) {
	if (err) {
		console.error('ERROR: %s', VE.fullStack(err));
		process.exit(1);
	}
});
