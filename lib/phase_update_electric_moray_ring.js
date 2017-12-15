
var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');

var VE = mod_verror.VError;

function
phase_update_electric_moray_ring(ctl)
{
	var p = ctl.plan();

	var hold = false;

	var new_ring = ctl.prop_get('new_hash_ring_uuid');
	mod_assert.uuid(new_ring, 'new_ring');

	mod_vasync.waterfall([ function (done) {
		ctl.get_instances({ service: 'electric-moray' }, done);

	}, function (insts, done) {
		var list = Object.keys(insts).sort().map(function (uuid) {
			return (insts[uuid]);
		});

		ctl.log.info('updating Electric Moray instances');
		mod_vasync.forEachPipeline({ inputs: list,
		    func: function (inst, next) {
			update_one(ctl, inst, next);

		}}, function (err) {
			done(err);
		});

	} ], function (err) {
		if (err) {
			if (hold) {
				ctl.hold(err);
			} else {
				ctl.retry(err);
			}
			return;
		}

		ctl.finish();
	});
}

function
get_deployed_hash_version(ctl, zone, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.uuid(zone, 'zone');
	mod_assert.func(callback, 'callback');

	var smfi = {};
	var check;
	var fmri = 'svc:/smartdc/application/electric-moray';

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

		ctl.log.info({ instances: list }, 'found %d instances',
		    list.length);

		list.forEach(function (i) {
			smfi[i] = {
				smfi_fmri: i,
				smfi_path: null,
				smfi_stamp: null
			};
		});

		setImmediate(done);

	}, function (done) {
		/*
		 * Determine the hash database location for each instance.
		 */
		mod_vasync.forEachPipeline({ inputs: Object.keys(smfi),
		    func: function (i, next) {
			lib_common.get_zone_smf_propval(ctl, zone, i,
			    'electric-moray/ring-location',
			    function (err, path) {
				if (err) {
					next(err);
					return;
				}

				smfi[i].smfi_path = path;

				ctl.log.info('instance "%s" has hash path ' +
				    '"%s"', i, smfi[i].smfi_path);
				next();
			});
		}}, function (err) {
			done(err);
		});

	}, function (done) {
		/*
		 * Determine the hash database stamp for each instance.
		 */
		mod_vasync.forEachPipeline({ inputs: Object.keys(smfi),
		    func: function (i, next) {
			ctl.log.info('reading hash ring stamp for instance ' +
			    '"%s" in zone "%s"', i, zone);
			ctl.zone_exec(zone, 'json -f "' +
			    smfi[i].smfi_path + '/manta_hash_ring_stamp.json"',
			    function (err, res) {
				if (err) {
					next(err);
					return;
				}

				if (res.exit_status !== 0) {
					if (res.stderr.trim().match(/ENOENT/)) {
						smfi[i].smfi_stamp = false;
						ctl.log.info('no stamp');
						next();
						return;
					}

					next(new VE('read failed'));
					return;
				}

				var stamp;
				try {
					stamp = JSON.parse(res.stdout.trim());
				} catch (ex) {
					next(new VE(ex, 'parse failed'));
					return;
				}

				smfi[i].smfi_stamp = stamp;

				/* XXX */
				ctl.log.info({ stamp: stamp }, 'stamp');

				next();
			});
		}}, function (err) {
			done(err);
		});

	}, function (done) {
		/*
		 * Check that each instance has the same stamp.
		 */
		var list = Object.keys(smfi).sort().map(function (k) {
			return (smfi[k]);
		});
		mod_assert.ok(list.length > 0, 'there must be instances');

		check = list[0].smfi_stamp;
		if (check !== false) {
			mod_assert.object(check, 'check stamp');
		}
		for (var i = 1; i < list.length; i++) {
			var stamp = list[i].smfi_stamp;

			if (!mod_jsprim.deepEqual(check, stamp)) {
				done(new VE('inconsistent hash rings in ' +
				    'zone "%s"', zone));
				return;
			}
		}

		setImmediate(done, null);

	} ], function (err) {
		if (err) {
			ctl.log.info(err, 'failed to get hash ring version ' +
			    'from zone "%s" (retrying)', zone);
			setTimeout(function () {
				get_deployed_hash_version(ctl, zone, callback);
			}, 5000);
			return;
		}

		callback(null, check);
	});
}

function
update_one(ctl, inst, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.object(inst, 'inst');
	mod_assert.func(callback, 'callback');

	var expected_ring = ctl.prop_get('new_hash_ring_uuid');
	mod_assert.uuid(expected_ring, 'expected_ring');

	var reprovision = false;

	mod_vasync.waterfall([ function (done) {
		ctl.log.info('checking hash ring in electric moray "%s"',
		    inst.uuid);
		get_deployed_hash_version(ctl, inst.uuid, function (err, st) {
			if (err) {
				done(err);
				return;
			}

			if (st === false) {
				ctl.log.info('no stamp; old hash ring!');
				reprovision = true;
				done();
				return;
			}

			mod_assert.object(st, 'st');
			mod_assert.uuid(st.image_uuid, 'st.image_uuid');

			if (st.image_uuid !== expected_ring) {
				ctl.log.info('image "%s" is not expected "%s"' +
				    st.image_uuid, expected_ring);
				reprovision = true;
				done();
				return;
			}

			ctl.log.info('no reprovision required');
			done();
		});

	}, function (done) {
		if (!reprovision) {
			setImmediate(done);
			return;
		}

		ctl.log.info('disable registrar in instance "%s"', inst.uuid);
		ctl.zone_exec(inst.uuid, 'svcadm disable -s ' +
		    '"svc:/manta/application/registrar:default"',
		    function (err, res) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info({ result: res }, 'disable output');

			if (res.exit_status !== 0) {
				done(new VE('disable registrar'));
				return;
			}

			ctl.log.info('sleeping for services to fall out of ' +
			    'DNS');
			setTimeout(done, 60 * 1000);
		});

	}, function (done) {
		if (!reprovision) {
			setImmediate(done);
			return;
		}

		ctl.log.info('reprovisioning electric moray "%s"', inst.uuid);
		ctl.reprovision_same_image(inst.uuid, done);

	}, function (done) {
		if (!reprovision) {
			setImmediate(done);
			return;
		}

		ctl.log.info('checking upgrade in electric moray "%s"',
		    inst.uuid);
		get_deployed_hash_version(ctl, inst.uuid, function (err, st) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info({ stamp: st}, 'after-repro check');

			if (st === false) {
				ctl.log.info('old hash ring (fail!)');
				done(new VE({ info: { hold: true }},
				    'zone "%s" has old hash ring after ' +
				    'reprovision', inst.uuid));
				return;
			}

			mod_assert.uuid(st.image_uuid, 'st.image_uuid');
			if (st.image_uuid !== expected_ring) {
				ctl.log.info('wrong hash ring (%s)',
				    st.image_uuid);
				done(new VE({ info: { hold: true }},
				    'zone "%s" has wrong hash ring (%s) ' +
				    'after reprovision', inst.uuid,
				    st.image_uuid));
				return;
			}

			ctl.log.info('zone "%s" reprovision with new ' +
			    'hash ring is OK!', inst.uuid);
			done();
		});

	} ], function (err) {
		callback();
	});
}

module.exports = {
	phase_update_electric_moray_ring: phase_update_electric_moray_ring,
};
