

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


if (process.argv.length !== 3 || process.argv[2].length !== 36) {
	console.error('ERROR: Usage: unhold <plan_uuid>');
	process.exit(1);
}

function
fetch_unhold(ip, port, plan, callback)
{
	mod_assert.ok(mod_net.isIPv4(ip), 'ip');
	mod_assert.number(port, 'port');
	mod_assert.func(callback, 'callback');

	var finished = false;

	var req = mod_http.request({
		host: ip,
		port: port,
		method: 'POST',
		path: '/plan/' + plan + '/unhold',
		agent: false,
	});

	var fail = function (err) {
		if (finished) {
			return;
		}
		finished = true;

		req.abort();

		console.error('unhold request to %s:%d failed', ip, port);

		callback(new VE(err, 'unhold request to %s:%d', ip, port));
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

fetch_unhold('127.0.0.1', 80, process.argv[2], function (err, res) {
	if (err) {
		console.error('ERROR: %s', VE.fullStack(err));
		process.exit(1);
	}

	console.log('result: %s', JSON.stringify(res, false, 4));
});
