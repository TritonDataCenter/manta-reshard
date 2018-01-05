

var mod_net = require('net');
var mod_http = require('http');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');
var mod_extsprintf = require('extsprintf');

var lib_common = require('../lib/common');
var lib_status = require('../lib/status');

var VE = mod_verror.VError;


function
fetch_phases(ip, port, callback)
{
	mod_assert.ok(mod_net.isIPv4(ip), 'ip');
	mod_assert.number(port, 'port');
	mod_assert.func(callback, 'callback');

	var finished = false;

	var req = mod_http.request({
		host: ip,
		port: port,
		method: 'GET',
		path: '/phases',
		agent: false,
	});

	var fail = function (err) {
		if (finished) {
			return;
		}
		finished = true;

		req.abort();

		console.error('phases request to %s:%d failed', ip, port);

		callback(new VE(err, 'phases request to %s:%d', ip, port));
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

fetch_phases('127.0.0.1', 80, function (err, res) {
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
