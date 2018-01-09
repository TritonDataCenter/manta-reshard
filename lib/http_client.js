

var mod_net = require('net');
var mod_http = require('http');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');

var VE = mod_verror.VError;

function
http_request(method, ip, port, path, body, callback)
{
	mod_assert.string(method, 'method');
	mod_assert.ok(mod_net.isIPv4(ip), 'ip');
	mod_assert.number(port, 'port');
	mod_assert.string(path, 'path');
	mod_assert.optionalObject(body, 'body');
	mod_assert.func(callback, 'callback');

	var finished = false;

	var body_buffer;
	var headers = {};

	if (body) {
		headers['content-type'] = 'application/json';
		body_buffer = new Buffer(JSON.stringify(body));
		headers['content-length'] = String(body_buffer.length);
	} else if (method === 'POST') {
		headers['content-length'] = 0;
	}

	var req = mod_http.request({
		host: ip,
		port: port,
		method: method,
		path: path,
		headers: headers,
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

			var o;
			var body_error;
			try {
				o = JSON.parse(body);
			} catch (ex) {
				body_error = ex;
			}

			if (!(res.statusCode >= 200 && res.statusCode <= 299)) {
				finish_request(new VE({ info: {
				    status_code: res.statusCode,
				    body: o }}, 'status code %d',
				    res.statusCode));
				return;
			}

			if (body_error) {
				finish_request(new VE(body_error,
				    'parse body'));
				return;
			}

			finish_request(null, o);
		});
	});

	if (body_buffer) {
		req.write(body_buffer);
	}

	req.end();
}

function
http_get(ip, port, path, callback)
{
	http_request('GET', ip, port, path, null, callback);
}

function
http_post(ip, port, path, body, callback)
{
	http_request('POST', ip, port, path, body, callback);
}

function
cmd_print_result(err, res)
{
	if (err) {
		var info = VE.info(err);

		if (info.body) {
			console.error('ERROR: %s; response: %s', err.message,
			    JSON.stringify(info.body, null, 4));
		} else {
			console.error('ERROR: %s', err.message);
		}

		process.exit(1);
	}

	console.log('result: %s', JSON.stringify(res, false, 4));
}

module.exports = {
	http_get: http_get,
	http_post: http_post,

	cmd_print_result: cmd_print_result
};
