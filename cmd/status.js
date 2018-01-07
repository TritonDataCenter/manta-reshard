

var mod_net = require('net');
var mod_http = require('http');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');
var lib_status = require('../lib/status');
var lib_http_client = require('../lib/http_client');

var VE = mod_verror.VError;


function
print_status(redraw, callback)
{
	lib_http_client.http_get('127.0.0.1', 80, '/plans',
	    function (err, res) {
		if (redraw) {
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

		var k = Object.keys(res.plans).sort();

		for (var i = 0; i < k.length; i++) {
			var p = res.plans[k[i]];

			lib_status.pretty_print(p.status);

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
	setTimeout(print_status, 500, true, redraw_callback);
}

var redraw = false;
if (process.argv[2] === '-r') {
	redraw = true;
}

print_status(redraw, redraw ? redraw_callback : function (err) {
	if (err) {
		console.error('ERROR: %s', VE.fullStack(err));
		process.exit(1);
	}
});
