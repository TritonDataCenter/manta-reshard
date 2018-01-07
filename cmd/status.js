

var mod_net = require('net');
var mod_http = require('http');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');
var lib_status = require('../lib/status');

var VE = mod_verror.VError;


function
fetch_status(ip, port, callback)
{
	mod_assert.ok(mod_net.isIPv4(ip), 'ip');
	mod_assert.number(port, 'port');
	mod_assert.func(callback, 'callback');

	var finished = false;

	var req = mod_http.request({
		host: ip,
		port: port,
		method: 'GET',
		path: '/plans',
		agent: false,
	});

	var fail = function (err) {
		if (finished) {
			return;
		}
		finished = true;

		req.abort();

		console.error('status request to %s:%d failed', ip, port);

		callback(new VE(err, 'status request to %s:%d', ip, port));
	};

	/*
	 * Operation timeout.
	 */
	var timeo = setTimeout(function () {
		fail(new VE('timed out'));
	}, 60 * 1000);

	req.on('error', function (err) {
		fail(new VE(err, 'request error'));
	});

	req.once('response', function (res) {
		var body = '';

		if (res.statusCode !== 200) {
			console.error('response status code %d',
			    res.statusCode);
		}

		res.on('error', function (err) {
			fail(new VE(err, 'response error'));
		});

		res.on('readable', function () {
			var d;

			while ((d = res.read()) !== null) {
				body += d.toString('utf8');
			}
		});

		res.on('end', function () {
			if (finished) {
				return;
			}

			var o;

			try {
				o = JSON.parse(body);
			} catch (ex) {
				fail(new VE(ex, 'parse body'));
				return;
			}

			clearTimeout(timeo);
			finished = true;
			callback(null, o);
		});
	});

	req.end();
}

function
print_status(redraw, callback)
{
	fetch_status('127.0.0.1', 80, function (err, res) {
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
