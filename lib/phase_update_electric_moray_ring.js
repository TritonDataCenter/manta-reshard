
var mod_path = require('path');
var mod_fs = require('fs');
var mod_http = require('http');
var mod_net = require('net');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_uuid = require('uuid');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');
var lib_template = require('../lib/template');
var lib_electric_moray = require('../lib/electric_moray');

var VE = mod_verror.VError;

function
phase_update_electric_moray_ring(ctl)
{
	var insts;
	var plan = ctl.plan();
	var status = ctl.status();
	var scripts = {};

	var opts = {
		HASH_RING_IMAGE: ctl.prop_get('new_hash_ring_uuid'),
	};
	mod_assert.uuid(opts.HASH_RING_IMAGE, 'new_hash_ring_uuid');

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		status.update('refreshing "manta" SAPI application');
		ctl.get_manta_app(done);

	}, function (app, done) {
		if (ctl.pausing(done)) {
			return;
		}

		mod_assert.strictEqual(app.metadata.HASH_RING_IMAGE,
		    opts.HASH_RING_IMAGE, 'inconsistent HASH_RING_IMAGE');
		opts.HASH_RING_IMGAPI_SERVICE =
		    app.metadata.HASH_RING_IMGAPI_SERVICE;

		status.update('listing instances of "electric-moray"');
		ctl.get_instances({ service: 'electric-moray' }, done);

	}, function (_insts, done) {
		if (ctl.pausing(done)) {
			return;
		}

		insts = _insts;

		var ilist = Object.keys(insts).sort();
		var cnt = 0;

		status.update('updating hash ring in all Electric Moray ' +
		    'instances');
		ctl.log.info({ instances: ilist },
		    'updating hash ring in all Electric Moray instances');

		mod_vasync.forEachPipeline({ inputs: ilist,
		    func: function (uuid, next) {
			if (ctl.pausing(next)) {
				return;
			}

			var stch = status.child();
			cnt++;
			stch.update('zone %s (%d/%d)', uuid, cnt, ilist.length);

			ctl.log.info('updating hash ring in Electric Moray ' +
			    'in zone "%s"', uuid);
			update_one(ctl, uuid, opts, stch, function (err) {
				stch.trunc();
				if (err) {
					stch.child().update('failed: %s',
					    err.message);
				} else {
					stch.child().update('ok');
				}
				next(err);
			});

		}}, function (err) {
			done(err);
		});

	} ], function (err) {
		if (err) {
			ctl.retry(err);
			return;
		}

		ctl.finish();
	});
}

function
update_one(ctl, zone, opts, status, callback)
{
	var fmri = 'svc:/smartdc/application/electric-moray';
	var asmf;

	var stch = status.child();

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Make sure zone setup has completed before we interrogate
		 * SMF.
		 */
		stch.update('checking for completed zone setup');
		lib_common.check_zone_setup_complete(ctl, zone, done);

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Enumerate the set of Electric Moray SMF instances in this
		 * zone.
		 */
		stch.update('listing SMF instances');
		lib_common.get_zone_smf_instances(ctl, zone, fmri, done);

	}, function (list, done) {
		if (ctl.pausing(done)) {
			return;
		}

		if (list.length < 1) {
			done(new VE('did not find any SMF instances!'));
			return;
		}

		asmf = list.map(function (f) {
			return ({
				smf_fmri: f,
				smf_ip: null,
				smf_port: null,
				smf_path: null,
				smf_stamp: null,
				smf_status: status.child(),
			});
		});

		stch.update('determining zone IP address');
		ctl.get_instance_ip(zone, done);

	}, function (ip, done) {
		if (ctl.pausing(done)) {
			return;
		}

		mod_assert.ok(mod_net.isIPv4(ip, 'ip'));

		ctl.log.info('zone "%s" has IP %s', zone, ip);
		asmf.forEach(function (smf) {
			smf.smf_ip = ip;
		});

		stch.done();

		/*
		 * For each instance, we want to know the port on which Electric
		 * Moray is listening.  We will use this to calculate the
		 * port of the status API.
		 */
		mod_vasync.forEachPipeline({ inputs: asmf,
		    func: function (smf, next) {
			if (ctl.pausing(next)) {
				return;
			}

			smf.smf_status.update('%s', smf.smf_fmri);

			var ch = smf.smf_status.child();
			ch.update('fetching status port number');

			lib_common.get_zone_smf_propval(ctl, zone, smf.smf_fmri,
			    'electric-moray/status', function (err, port) {
				ch.done();
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

				smf.smf_port = num;

				next();
			});

		}}, function (err) {
			done(err);
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * For each instance, we want to know the hash ring path
		 * We will use this to check the currently deployed hash
		 * ring image stamp.
		 */
		mod_vasync.forEachPipeline({ inputs: asmf,
		    func: function (smf, next) {
			if (ctl.pausing(next)) {
				return;
			}

			var ch = smf.smf_status.child();
			ch.update('fetching ring location');

			lib_common.get_zone_smf_propval(ctl, zone, smf.smf_fmri,
			    'electric-moray/ring-location',
			    function (err, path) {
				ch.done();

				if (err) {
					next(err);
					return;
				}

				smf.smf_path = path;

				next();
			});

		}}, function (err) {
			done(err);
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		mod_vasync.forEachPipeline({ inputs: asmf,
		    func: function (smf, next) {
			if (ctl.pausing(next)) {
				return;
			}

			restart_until_ok(ctl, zone, smf, opts, next);
		}}, function (err) {
			done(err);
		});

	} ], function (err) {
		callback(err);
	});
}

function
restart_until_ok(ctl, zone, smf, script_opts, callback)
{
	var p = ctl.plan();

	var complete = false;
	var script;

	smf.smf_status.trunc();
	var status = smf.smf_status.child();

	script_opts = mod_jsprim.deepCopy(script_opts);
	script_opts.FMRI = smf.smf_fmri;

	var expected_ring = ctl.prop_get('new_hash_ring_uuid');
	mod_assert.uuid(expected_ring, 'expected_ring');

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		status.update('loading script templates');
		lib_template.template_load('electric-moray-update-ring.sh',
		    done);

	}, function (_script, done) {
		if (ctl.pausing(done)) {
			return;
		}

		script = _script;

		/*
		 * Determine the current hash database stamp for this instance.
		 */
		status.update('checking current hash ring version');
		ctl.log.info('checking hash stamp for "%s" in zone "%s"',
		    smf.smf_fmri, zone);
		ctl.zone_exec(zone, 'json -f "' + smf.smf_path +
		    '/manta_hash_ring_stamp.json"',
		    function (err, res) {
			if (err) {
				done(err);
				return;
			}

			if (res.exit_status !== 0) {
				if (res.stderr.trim().match(/ENOENT/)) {
					smf.smf_stamp = false;
					ctl.log.info('no stamp');
					done();
					return;
				}

				done(new VE('read failed: %s',
				    res.stderr.trim()));
				return;
			}

			var stamp;
			try {
				stamp = JSON.parse(res.stdout.trim());
			} catch (ex) {
				done(new VE(ex, 'parse failed'));
				return;
			}

			/*
			 * XXX
			 */
			ctl.log.info({ stamp: stamp }, 'stamp');
			smf.smf_stamp = stamp;

			done();
		});

	}, function (done) {
		if (smf.smf_stamp === false) {
			ctl.log.info('no stamp; old hash ring!');
			setImmediate(done);
			return;
		}

		mod_assert.object(smf.smf_stamp, 'smf_stamp');
		mod_assert.uuid(smf.smf_stamp.image_uuid, 'stamp image_uuid');

		if (smf.smf_stamp.image_uuid !== expected_ring) {
			ctl.log.info('image "%s" is not expected "%s"',
			    smf.smf_stamp.image_uuid,
			    expected_ring);
			setImmediate(done);
			return;
		}

		ctl.log.info('correct hash ring already installed!');
		complete = true;
		setImmediate(done);

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		if (complete) {
			setImmediate(done);
			return;
		}

		status.update('updating hash ring database');
		ctl.log.info('updating Electric Moray instance "%s" in ' +
		    'zone "%s"', smf.smf_fmri, zone);
		ctl.zone_exec(zone, script.render(script_opts),
		    function (err, res) {
			if (err) {
				done(err);
				return;
			}

			if (res.exit_status !== 0) {
				done(new VE('update failed: %s',
				    res.stderr.trim()));
				return;
			}

			done();
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Check status endpoint to ensure this instance is
		 * working correctly.  Wait for at most 120 seconds.
		 */
		var wait_start = process.hrtime();

		status.update('waiting for instance to be online');
		ctl.log.info('waiting for instance "%s" in zone "%s" to ' +
		    'be online', smf.smf_fmri, zone);

		var wait_for_online = function () {
			if (ctl.pausing(done)) {
				return;
			}

			lib_electric_moray.em_fetch_status(ctl,
			    smf.smf_ip, smf.smf_port,
			    function (err, res) {
				if (ctl.pausing(done)) {
					return;
				}

				if (err) {
					var delta = mod_jsprim.hrtimeMillisec(
					    process.hrtime(wait_start));

					if (delta > 120 * 1000) {
						ctl.log.warn(err,
						    'giving up!');
						var info = { hold: true };
						done(new VE({ info: info,
						    cause: err },
						    'timed out waiting ' +
						    'for electric moray'));
						return;
					}

					/*
					 * XXX
					 */
					status.prop('message',
					    err.message);
					ctl.log.info('still waiting');
					setTimeout(wait_for_online, 1000);
					return;
				}

				status.clear();
				done();
			});
		};

		wait_for_online();

	} ], function (err) {
		if (err) {
			if (VE.info(err).hold) {
				callback(err);
				return;
			}

			if (VE.info(err).exe_pause) {
				/*
				 * If this is a pause request, do not retry.
				 */
				callback(err);
				return;
			}

			status.update('waiting to retry: %s', err.message);
			ctl.log.info(err, 'retrying');
			setTimeout(function () {
				restart_until_ok(ctl, zone, smf, script_opts,
				    callback);
			}, 5000);
			return;
		}

		if (complete) {
			status.update('update ok');
			ctl.log.info('Electric Moray instance "%s" has ' +
			    'correct read-only status', zone);
			callback();
			return;
		}

		if (ctl.pausing(callback)) {
			return;
		}

		/*
		 * We need to complete at least one status fetch with the
		 * correct view of vnodes.
		 */
		status.update('waiting to confirm hash ring updated');
		setTimeout(function () {
			ctl.log.info('checking for updated Electric ' +
			    'Moray status');
			restart_until_ok(ctl, zone, smf, script_opts, callback);
		}, 5000);
	});
}

module.exports = {
	phase_update_electric_moray_ring: phase_update_electric_moray_ring,
};
