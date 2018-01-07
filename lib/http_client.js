

var mod_net = require('net');
var mod_http = require('http');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');

var VE = mod_verror.VError;

function
http_request(method, ip, port, path, callback)
{
	mod_assert.string(method, 'method');
	mod_assert.ok(mod_net.isIPv4(ip), 'ip');
	mod_assert.number(port, 'port');
	mod_assert.string(path, 'path');
	mod_assert.func(callback, 'callback');

	var finished = false;

	var req = mod_http.request({
		host: ip,
		port: port,
		method: method,
		path: path,
		agent: false,
	});

	var timeo;
	var finish_request = function (err, obj) {
		if (finished) {
			return;
		}
		finished = true;

		clearTimeout(timeo);

		if (!err) {
			callback(null, obj);
			return;
		}

		req.abort();

		var i = { info: { host: ip, port: port, method: method,
		    path: path }, cause: err };
		callback(new VE(i, '%s http://%s:%d%s', method, ip, port,
		    path));
	};

	/*
	 * Operation timeout.
	 */
	timeo = setTimeout(function () {
		finish_request(new VE('timed out'));
	}, 60 * 1000);

	req.on('error', function (err) {
		finish_request(new VE(err, 'request error'));
	});

	req.once('response', function (res) {
		var body = '';

		res.on('error', function (err) {
			finish_request(new VE(err, 'response error'));
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

			if (!(res.statusCode >= 200 && res.statusCode <= 299)) {
				finish_request(new VE({ info: {
				    status_code: res.statusCode }},
				    'status code %d', res.statusCode));
				return;
			}

			var o;
			try {
				o = JSON.parse(body);
			} catch (ex) {
				finish_request(new VE(ex, 'parse body'));
				return;
			}

			finish_request(null, o);
		});
	});

	req.end();
}

function
http_get(ip, port, path, callback)
{
	http_request('GET', ip, port, path, callback);
}

function
http_post(ip, port, path, callback)
{
	http_request('POST', ip, port, path, callback);
}

module.exports = {
	http_get: http_get,
	http_post: http_post,
};
