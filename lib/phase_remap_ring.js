
var mod_path = require('path');
var mod_fs = require('fs');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_uuid = require('uuid');
var mod_jsprim = require('jsprim');

var VE = mod_verror.VError;

var TEMPLATES = [
	'hashring-prime-workspace',
	'hashring-remap-vnodes',
	'hashring-create-archive',
];

function
template(scripts, name, opts)
{
	mod_assert.ok(TEMPLATES.indexOf(name) !== -1, 'template ' + name);

	var out = scripts[name];

	Object.keys(opts).forEach(function (k) {
		var re = new RegExp('%%' + k + '%%', 'g');

		mod_assert.string(opts[k], 'opts.' + k);

		out = out.replace(re, opts[k]);
	});

	return (out);
}

function
phase_remap_ring(ctl)
{
	var insts;
	var plan = ctl.plan();
	var scripts = {};

	/*
	 * Select a workspace ID for this process.  The ID should be unique,
	 * to prevent other processes (or subsequent instances of this process)
	 * from colliding.
	 */
	var short_random = mod_uuid.v4().substr(0, 8);
	var opts = {
		WORKSPACE_ID: plan.uuid + '.' + short_random,
		TRANSITION: 'split shard '+ plan.shard + ' in half; create ' +
		    'new shard ' + plan.new_shard,
		PLAN_UUID: plan.uuid,
		SHARD: plan.shard,
		NEW_SHARD: plan.new_shard,
	};

	var existing_image = null;

	mod_vasync.waterfall([ function (done) {
		ctl.get_manta_app(function (err, app) {
			if (err) {
				done(err);
				return;
			}

			opts.HASH_RING_IMGAPI_SERVICE =
			    app.metadata.HASH_RING_IMGAPI_SERVICE;
			opts.HASH_RING_IMAGE = app.metadata.HASH_RING_IMAGE;

			mod_assert.uuid(app.owner_uuid, 'owner_uuid');
			opts.POSEIDON_UUID = app.owner_uuid;

			ctl.log.info({ script_opts: opts }, 'script options');

			done();
		});

	}, function (done) {
		/*
		 * Request the list of images from IMGAPI to see if we have
		 * already uploaded one for this plan.
		 */
		ctl.log.info('checking for existing updated hash ring image');
		ctl.list_images({ name: 'manta-hash-ring',
		    owner: opts.POSEIDON_UUID }, function (err, images) {
			if (err) {
				done(err);
				return;
			}

			var our_images = images.filter(function (image) {
				if (!image.tags) {
					return (false);
				}

				return (image.tags.manta_reshard_plan ===
				    plan.uuid);
			});

			if (our_images.length === 0) {
				ctl.log.info('existing image not found');
				setImmediate(done);
				return;
			}

			if (our_images.length === 1) {
				ctl.log.info({ image: our_images[0] },
				    'existing image found!');
				existing_image = our_images[0].uuid;
				mod_assert.uuid(existing_image,
				    'existing_image');
				setImmediate(done);
				return;
			}

			ctl.log.warn({ images: our_images },
			    'found more than one image for our reshard plan');
			done(new VE('found multiple images for our plan: %j',
			    our_images.map(function (i) { return (i.uuid); })));
		});

	}, function (done) {
		if (existing_image !== null) {
			setImmediate(done);
			return;
		}

		ctl.get_instances({ service: 'electric-moray' },
		    function (err, _insts) {
			if (err) {
				done(err);
				return;
			}

			insts = _insts;
			done();
		});

	}, function (done) {
		if (existing_image !== null) {
			setImmediate(done);
			return;
		}

		/*
		 * Load template scripts.
		 */
		var dir = mod_path.join(__dirname, '..', 'templates');
		mod_vasync.forEachPipeline({ inputs: TEMPLATES,
		    func: function (tmpl, cb) {
			var file = mod_path.join(dir, tmpl + '.sh');
			mod_fs.readFile(file, { encoding: 'utf8' },
			    function (err, data) {
				if (err) {
					cb(new VE(err, 'read template "%s"',
					    file));
					return;
				}

				ctl.log.info('loaded template "%s"', tmpl);
				scripts[tmpl] = data;
				cb();
			});
		}}, function (err) {
			done(err);
		});

	}, function (done) {
		if (existing_image !== null) {
			setImmediate(done);
			return;
		}

		var i = insts[Object.keys(insts)[0]];

		/*
		 * Create a workspace in an Electric Moray zone.  Download
		 * and unpack a copy of the latest hash ring database.
		 */
		ctl.log.info('using Electric Moray zone %s for unpack',
		    i.uuid);
		ctl.zone_exec(i.uuid, template(scripts,
		    'hashring-prime-workspace', opts), function (err, res) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info({ result: res }, 'output');

			if (res.exit_status !== 0) {
				done(new VE('prime workspace failed'));
				return;
			}

			done();
		});

	}, function (done) {
		if (existing_image !== null) {
			setImmediate(done);
			return;
		}

		/*
		 * We want to include log messages from the running script.
		 * XXX we also want to use this for control flow information
		 */
		var called_back = false;
		var check;
		var start_at = process.hrtime();
		var last_post = null;
		var puts = 0;
		var handle_post = function (body, callback) {
			mod_assert.object(body, 'body');
			mod_assert.func(callback, 'callback');

			if (called_back) {
				callback(new VE('POST after end'));
				return;
			}

			puts++;
			last_post = process.hrtime();

			ctl.log.info({
				runtime_ms: mod_jsprim.hrtimeMillisec(
				    process.hrtime(start_at)),
				count: puts,
				status: body
			}, 'status from script: %s', body.message);

			if (body.error) {
				ctl.log.info('script failed: %s', body.error);
				var err = new VE('script failed: %s',
				    body.error);
				called_back = true;
				setImmediate(callback);
				done(err);
				return;
			}

			if (body.finished) {
				ctl.log.info('script finished!');
				called_back = true;
				setImmediate(callback);
				done();
				return;
			}

			setImmediate(callback);
		};
		opts.STATUS_URL = ctl.register_http_handler(handle_post);

		var i = insts[Object.keys(insts)[0]];

		check = setInterval(function () {
			if (called_back) {
				clearInterval(check);
				return;
			}

			if (last_post === null) {
				/*
				 * If we have not yet heard from the script,
				 * wait for an Ur failure below.
				 */
				mod_assert.strictEqual(puts, 0, 'puts 0');
				return;
			}

			/*
			 * Determine how long it has been since we last heard
			 * from the script.
			 */
			var runtime_ms = mod_jsprim.hrtimeMillisec(
			    process.hrtime(last_post));

			var secs = 300;
			if (runtime_ms > secs * 1000) {
				ctl.log.warn('no status for %d seconds; ' +
				    'aborting', secs);
				called_back = true;
				done(new VE('script timed out'));
				return;
			}
		}, 1000);

		/*
		 * Remap vnodes.
		 */
		ctl.log.info({ script_opts: opts }, 'remapping vnodes!');
		ctl.zone_exec(i.uuid, template(scripts,
		    'hashring-remap-vnodes', opts), function (err, res) {
			if (err) {
				var info = VE.info(err);

				if (info.exec_timeout && puts > 0) {
					/*
					 * If we have received at least one
					 * status message from the script,
					 * this is fine.  The script may run
					 * much longer than is reasonable for
					 * an Ur execution timeout, but it
					 * posts regular status updates.
					 */
					ctl.log.info('ignoring ur exec ' +
					    'timeout');
					return;
				}

				ctl.log.info(err, 'ur failure');
				called_back = true;
				done(err);
				return;
			}

			ctl.log.info({ result: res }, 'output');

			if (called_back) {
				return;
			}

			if (res.exit_status !== 0) {
				called_back = true;
				done(new VE('upload workspace'));
				return;
			}
		});

	}, function (done) {
		if (existing_image !== null) {
			setImmediate(done);
			return;
		}

		var i = insts[Object.keys(insts)[0]];

		/*
		 * Pack the modified archive and have the zone upload it to
		 * IMGAPI.
		 */
		ctl.log.info('uploading modified archive to IMGAPI');
		ctl.zone_exec(i.uuid, template(scripts,
		    'hashring-create-archive', opts), function (err, res) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info({ result: res }, 'output');

			if (res.exit_status !== 0) {
				done(new VE('upload workspace'));
				return;
			}

			var out = res.stdout.trim();
			if (out.length !== 36) {
				/*
				 * XXX
				 */
				done(new VE('invalid uuid from script: "%s"',
				    out));
				return;
			}

			existing_image = out;

			done();
		});

	}, function (done) {
		mod_assert.uuid(existing_image, 'existing_image');

		ctl.get_image(existing_image, function (err, image) {
			if (err) {
				done(new VE('checking existing image (%s)',
				    existing_image));
				return;
			}

			ctl.log.info({ image: image },
			    'checked hash ring image in IMGAPI');
			done();
		});

	} ], function (err) {
		if (err) {
			ctl.retry(err);
			return;
		}

		/*
		 * XXX
		 */
		mod_assert.uuid(existing_image, 'existing_image');
		ctl.prop_put('new_hash_ring_uuid', existing_image);

		ctl.finish();
	});
}

module.exports = {
	phase_remap_ring: phase_remap_ring,
};
