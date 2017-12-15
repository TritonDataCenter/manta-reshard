

var mod_net = require('net');
var mod_http = require('http');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var VE = mod_verror.VError;


function
em_fetch_status(ctl, ip, port, callback)
{
	mod_assert.ok(mod_net.isIPv4(ip), 'ip');
	mod_assert.number(port, 'port');
	mod_assert.func(callback, 'callback');

	var finished = false;

	ctl.log.info('fetching status from http://%s:%d', ip, port);

	var req = mod_http.request({
		host: ip,
		port: port,
		method: 'GET',
		path: '/status',
		agent: false,
	});

	var fail = function (err) {
		if (finished) {
			return;
		}
		finished = true;

		req.abort();

		ctl.log.info(err, 'status request to %s:%d failed', ip, port);

		callback(new VE(err, 'status request to %s:%d', ip, port));
	};

	/*
	 * Operation timeout.
	 */
	setTimeout(function () {
		fail(new VE('timed out'));
	}, 60 * 1000);

	req.on('error', function (err) {
		fail(new VE(err, 'request error'));
	});

	req.once('response', function (res) {
		var body = '';

		ctl.log.info('response status code %d', res.statusCode);

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

			ctl.log.info({ status: o }, 'status response');

			finished = true;
			callback(null, o);
		});
	});

	req.end();
}


module.exports = {
	em_fetch_status: em_fetch_status,
};
