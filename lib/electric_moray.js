

var mod_net = require('net');
var mod_http = require('http');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');

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

function
em_restart_one(ctl, zone, ensure_status_func, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.uuid(zone, 'zone');
	mod_assert.func(ensure_status_func, 'ensure_status_func');
	mod_assert.func(callback, 'callback');

	var fmri = 'svc:/smartdc/application/electric-moray';
	var asmf;

	mod_vasync.waterfall([ function (done) {
		/*
		 * Make sure zone setup has completed before we interrogate
		 * SMF.
		 */
		lib_common.check_zone_setup_complete(ctl, zone, done);

	}, function (done) {
		/*
		 * Enumerate the set of Electric Moray SMF instances in this
		 * zone.
		 */
		lib_common.get_zone_smf_instances(ctl, zone, fmri, done);

	}, function (list, done) {
		if (list.length < 1) {
			done(new VE('did not find any SMF instances!'));
			return;
		}

		asmf = list.map(function (f) {
			return ({
				smf_fmri: f,
				smf_ip: null,
				smf_port: null,
				smf_status: null,
			});
		});

		setImmediate(done);

	}, function (done) {
		/*
		 * For each instance, we want to know the port on which Electric
		 * Moray is listening.  We will use this to calculate the
		 * port of the status API.
		 */
		mod_vasync.forEachPipeline({ inputs: asmf,
		    func: function (smf, next) {
			lib_common.get_zone_smf_propval(ctl, zone, smf.smf_fmri,
			    'electric-moray/port', function (err, port) {
				if (err) {
					next(err);
					return;
				}

				var num = mod_jsprim.parseInteger(port,
				    { allowSign: false });
				if (num instanceof Error) {
					next(new VE(num, 'invalid port'));
					return;
				}

				/*
				 * The HTTP status API port is 2000 ports above
				 * the moray service port; e.g., a moray port
				 * of 2021 would correspond to a status port
				 * of 4021.
				 */
				smf.smf_port = num + 2000;

				next();
			});

		}}, function (err) {
			done(err);
		});

	}, function (done) {
		ctl.get_instance_ip(zone, done);

	}, function (ip, done) {
		mod_assert.ok(mod_net.isIPv4(ip, 'ip'));

		ctl.log.info('zone "%s" has IP %s', zone, ip);
		asmf.forEach(function (smf) {
			smf.smf_ip = ip;
		});

		mod_vasync.forEachPipeline({ inputs: asmf,
		    func: function (smf, next) {
			em_restart_until_ok(ctl, zone, smf, ensure_status_func,
			    next);
		}}, function (err) {
			done(err);
		});

	} ], function (err) {
		callback(err);
	});
}

function
em_restart_until_ok(ctl, zone, smf, ensure_status_func, callback)
{
	var p = ctl.plan();

	var complete = false;

	mod_vasync.waterfall([ function (done) {
		em_fetch_status(ctl, smf.smf_ip, smf.smf_port, done);

	}, function (status, done) {
		if (status.smf_fmri !== smf.smf_fmri) {
			done(new VE('status FMRI was "%s", not "%s"',
			    status.smf_fmri, smf.smf_fmri));
			return;
		}

		var lookup = function (shard) {
			for (var i = 0; i < status.index_shards.length; i++) {
				var s = status.index_shards[i];

				if (s.host === shard) {
					return (s);
				}
			}

			return (null);
		};

		/*
		 * Check the index shard map to make sure this process
		 * has the expected view of the hash ring.
		 */
		if (ensure_status_func(ctl, lookup)) {
			ctl.log.info('status is OK; no restart required');
			complete = true;
		} else {
			ctl.log.info('Electric Moray requires restart');
		}

		setImmediate(done);

	}, function (done) {
		if (complete) {
			setImmediate(done);
			return;
		}

		ctl.log.info('restarting Electric Moray instance "%s" in ' +
		    'zone "%s"', smf.smf_fmri, zone);
		ctl.zone_exec(zone, 'svcadm restart ' + smf.smf_fmri,
		    function (err, res) {
			if (err) {
				done(err);
				return;
			}

			if (res.exit_status !== 0) {
				done(new VE('svcadm failed: %s',
				    res.stderr.trim()));
				return;
			}

			done();
		});

	}, function (done) {
		if (complete) {
			setImmediate(done);
			return;
		}

		ctl.log.info('waiting for instance "%s" in zone "%s" to ' +
		    'be online', smf.smf_fmri, zone);

		var wait_for_online = function () {
			lib_common.check_zone_smf_online(ctl, zone,
			    smf.smf_fmri, function (err) {
				if (err) {
					ctl.log.info('still waiting');
					setTimeout(wait_for_online, 5000);
					return;
				}

				done();
			});
		};

		wait_for_online();

	} ], function (err) {
		if (err) {
			ctl.log.info(err, 'retrying');
			setTimeout(function () {
				em_restart_until_ok(ctl, zone, smf,
				    ensure_status_func, callback);
			}, 5000);
			return;
		}

		if (complete) {
			ctl.log.info('Electric Moray instance "%s" has ' +
			    'correct read-only status', zone);
			callback();
			return;
		}

		/*
		 * We need to complete at least one status fetch with the
		 * correct view of vnodes.
		 */
		setTimeout(function () {
			ctl.log.info('checking for updated Electric ' +
			    'Moray status');
			em_restart_until_ok(ctl, zone, smf, ensure_status_func,
			    callback);
		}, 5000);
	});
}


module.exports = {
	em_fetch_status: em_fetch_status,
	em_restart_one: em_restart_one,
};
