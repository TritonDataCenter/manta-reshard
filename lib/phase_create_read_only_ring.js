
var mod_path = require('path');
var mod_fs = require('fs');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_uuid = require('uuid');

var VE = mod_verror.VError;

var TEMPLATES = [
	'hashring-prime-workspace',
	'hashring-mark-readonly',
	'hashring-get-status',
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
phase_create_read_only_ring(ctl)
{
	var insts;
	var plan = ctl.plan();
	var scripts = {};
	var em_uuid;

	/*
	 * Select a workspace ID for this process.  The ID should be unique,
	 * to prevent other processes (or subsequent instances of this process)
	 * from colliding.
	 */
	var short_random = mod_uuid.v4().substr(0, 8);
	var opts = {
		WORKSPACE_ID: plan.uuid + '.' + short_random
	};

	var md5;
	var existing_ring = ctl.prop_get('ring_readonly');

	if (existing_ring !== null) {
		ctl.log.info('found existing readonly ring: %s', existing_ring);
	}

	mod_vasync.waterfall([ function (done) {
		ctl.get_manta_app(function (err, app) {
			if (err) {
				done(err);
				return;
			}

			if (existing_ring !== null &&
			    ctl.prop_get('ring_readonly_parent') !==
			    app.metadata.HASH_RING_IMAGE) {
				ctl.log.info('existing ring created from ' +
				    'different source ring image; discarding');
				existing_ring = null;
				ctl.prop_del('ring_readonly');
				ctl.prop_del('ring_readonly_md5');
				ctl.prop_del('ring_readonly_parent');
			}

			opts.HASH_RING_IMGAPI_SERVICE =
			    app.metadata.HASH_RING_IMGAPI_SERVICE;
			opts.HASH_RING_IMAGE = app.metadata.HASH_RING_IMAGE;

			ctl.log.info({ script_opts: opts }, 'script options');

			done();
		});

	}, function (done) {
		if (existing_ring !== null) {
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
		if (existing_ring !== null) {
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
		if (existing_ring !== null) {
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
		/*
		 * XXX we need to actually _mark_ all the vnodes read-only
		 * here.  It takes quite a while on my test machine, so I've
		 * left it out for now.
		 */
		setImmediate(done);

	}, function (done) {
		if (existing_ring !== null) {
			setImmediate(done);
			return;
		}

		var i = insts[Object.keys(insts)[0]];

		/*
		 * Pack the modified archive and have the zone upload it to
		 * us.
		 */
		opts.PUT_FILE_URL = ctl.put_url(short_random + '.tar.gz');
		ctl.log.info({ script_opts: opts }, 'script options');

		ctl.log.info('requesting modified archive');
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

			md5 = res.stdout.trim();
			ctl.log.info('file MD5 = %s', md5);

			done();
		});

	}, function (done) {
		var fname = short_random + '.tar.gz';

		if (existing_ring !== null) {
			fname = existing_ring;
			md5 = ctl.prop_get('ring_readonly_md5');
		}

		ctl.log.info('verifying file "%s" has MD5 "%s"', fname, md5);

		ctl.verify_file(fname, md5, function (err) {
			if (err) {
				done(err);
				return;
			}

			ctl.prop_put('ring_readonly', short_random + '.tar.gz');
			ctl.prop_put('ring_readonly_md5', md5);
			ctl.prop_put('ring_readonly_parent',
			    opts.HASH_RING_IMAGE);

			done();
		});

	} ], function (err) {
		if (err) {
			ctl.retry(err);
			return;
		}

		ctl.finish();
	});
}

module.exports = {
	phase_create_read_only_ring: phase_create_read_only_ring,
};
