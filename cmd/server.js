
var mod_fs = require('fs');
var mod_path = require('path');

var mod_assert = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var mod_jsprim = require('jsprim');
var mod_ufds = require('ufds');
var mod_sdc = require('sdc-clients');
var mod_urclient = require('urclient');

var lib_data_access = require('../lib/data_access');
var lib_http_server = require('../lib/http_server');
var lib_executor = require('../lib/executor');


var VE = mod_verror.VError;

function
retry(func, ctx, done, nsecs)
{
	setTimeout(function () {
		func(ctx, done);
	}, nsecs * 1000);
}

function
get_sapi_application(log, sapi, name, include_master, callback)
{
	mod_assert.object(log, 'log');
	mod_assert.object(sapi, 'sapi');
	mod_assert.string(name, 'name');
	mod_assert.bool(include_master, 'include_master');
	mod_assert.func(callback, 'callback');

	var opts = {
		name: name
	};

	if (include_master) {
		/*
		 * Note that SAPI merely checks for the _presence_ of
		 * "include_master" in the query string.  Passing any value,
		 * even "false", results in the inclusion of master results.
		 */
		opts.include_master = true;
	}

	sapi.listApplications(opts, function (err, apps) {
		if (err) {
			callback(new VE(err, 'locate "%s" application', name));
			return;
		}

		if (apps.length !== 1) {
			callback(new VE('found %d "%s" applications, wanted 1',
			    apps.length, name));
			return;
		}

		callback(null, apps[0]);
	});
}


function
load_config(ctx, done)
{
	ctx.ctx_log.debug('loading configuration file "%s"', ctx.ctx_cfgfile);
	mod_fs.readFile(ctx.ctx_cfgfile, function (err, data) {
		if (err) {
			if (err.code === 'ENOENT') {
				if (ctx.ctx_cfgfile_notfound++ === 0) {
					ctx.ctx_log.info('waiting for ' +
					    'config file "%s"',
					    ctx.ctx_cfgfile);
				}

				/*
				 * Wait for the file to exist.
				 */
				retry(load_config, ctx, done, 1);
				return;
			}

			done(new VE(err, 'loading file "%s"', ctx.ctx_cfgfile));
			return;
		}

		var out;
		try {
			out = JSON.parse(data.toString('utf8'));
		} catch (ex) {
			done(new VE(ex, 'parsing file "%s"', ctx.ctx_cfgfile));
			return;
		}

		ctx.ctx_log.info({ config: out },
		    'loaded configuration from "%s"', ctx.ctx_cfgfile);

		ctx.ctx_cfg = out;

		setImmediate(done);
	});
}

function
create_ufds_client(ctx, done)
{
	var ufdsopts = mod_jsprim.deepCopy(ctx.ctx_cfg.ufds);

	ufdsopts.log = ctx.ctx_log.child({ component: 'ufds' });

	ctx.ctx_ufds = new mod_ufds(ufdsopts);

	var finished = false;
	var finish = function (err) {
		if (finished) {
			return;
		}
		finished = true;

		if (err) {
			done(new VE(err, 'could not connect to UFDS'));
		} else {
			done();
		}
	};

	ctx.ctx_ufds.once('connect', finish);
	ctx.ctx_ufds.once('connectError', finish);
}

function
create_sapi_client(ctx, done)
{
	ctx.ctx_sapi = new mod_sdc.SAPI({
		url: ctx.ctx_cfg.sapi_url,
		log: ctx.ctx_log.child({ component: 'sapi' }),
		agent: false
	});

	setImmediate(done);
}

function
find_datacentres(ctx, done)
{
	ctx.ctx_ufds.listDatacenters(ctx.ctx_cfg.region, function (err, dcs) {
		if (err) {
			ctx.ctx_log.warn(err, 'could not list ' +
			    'datacentres (retrying)');
			retry(find_datacentres, ctx, done, 5);
			return;
		}

		ctx.ctx_log.debug({ dcs: dcs }, 'datacentre list');

		var out = {};

		dcs.forEach(function (dc) {
			mod_assert.strictEqual(dc.region, ctx.ctx_cfg.region,
			    'unexpected region');
			mod_assert.string(dc.datacenter, 'dc.datacenter');
			mod_assert.ok(!out[dc.datacenter]);

			out[dc.datacenter] = {
				dc_region: dc.region,
				dc_name: dc.datacenter,
				dc_sapi: null,
				dc_cnapi: null,
				dc_app: null,
				dc_clients: {
					dcc_sapi: null,
					dcc_urclient: null,
				}
			};
		});

		ctx.ctx_log.info({ dcs: out }, 'found datacentres');
		ctx.ctx_dcs = out;

		done();
	}, true);
}

/*
 * Use our local SAPI to get a list of all instances of all Manta services.
 */
function
load_manta_application(ctx, done)
{
	get_sapi_application(ctx.ctx_log, ctx.ctx_sapi, 'manta', true,
	    function (err, app) {
		if (err) {
			ctx.ctx_log.warn(err, 'could not locate ' +
			    '"manta" SAPI application (retrying)');
			retry(load_manta_application, ctx, done, 5);
			return;
		}

		ctx.ctx_app_refresh = Date.now();
		ctx.ctx_app = app;

		ctx.ctx_log.info('found manta application "%s"', app.uuid);
		ctx.ctx_log.trace({ manta_app: app }, 'manta application');

		mod_assert.strictEqual(ctx.ctx_app.metadata.REGION,
		    ctx.ctx_cfg.region, 'REGION');

		done();
	});
}

function
load_manta_objects(ctx, done)
{
	ctx.ctx_sapi.getApplicationObjects(ctx.ctx_app.uuid,
	    { include_master: true }, function (err, res) {
		if (err) {
			ctx.ctx_log.warn(err, 'could not load ' +
			    'SAPI objects for "manta" application (retrying)');
			retry(load_manta_objects, ctx, done, 5);
			return;
		}

		mod_assert.object(res.services, 'services');
		mod_assert.object(res.instances, 'instances');

		ctx.ctx_log.trace({ services: res.services }, 'services!');

		ctx.ctx_services = res.services;
		ctx.ctx_instances = res.instances;

		done();
	});
}

function
find_sapi_urls(ctx, done)
{
	var err = null;

	mod_assert.object(ctx.ctx_instances, 'ctx_instances');

	mod_jsprim.forEachKey(ctx.ctx_instances, function (us, insts) {
		mod_jsprim.forEachKey(insts, function (ui, inst) {
			var dc = ctx.ctx_dcs[inst.metadata.DATACENTER];

			if (!dc) {
				err = new VE('instance "%s" is in unknown ' +
				    'DC "%s"', ui, inst.metadata.DATACENTER);
				return;
			}

			if (dc.dc_sapi !== null && dc.dc_sapi !==
			    inst.metadata.SAPI_URL) {
				err = new VE('instance "%s" has different ' +
				    'SAPI URL "%s"',
				    ui, inst.metadata.SAPI_URL);
				return;
			}

			dc.dc_sapi = inst.metadata.SAPI_URL;
		});
	});

	mod_jsprim.forEachKey(ctx.ctx_dcs, function (n, dc) {
		if (dc.dc_sapi === null) {
			err = new VE('could not find SAPI URL for DC "%s"', n);
			return;
		}

		ctx.ctx_log.info({ dc_sapi_url: dc.dc_sapi },
		     'found SAPI URL for DC "%s"', n);
	});

	if (err) {
		setImmediate(done, err);
		return;
	}

	ctx.ctx_log.info('found SAPI URLs for all DCs');
	setImmediate(done);
}

function
create_remote_sapi_clients(ctx, done)
{
	mod_jsprim.forEachKey(ctx.ctx_dcs, function (n, dc) {
		dc.dc_clients.dcc_sapi = new mod_sdc.SAPI({
			url: dc.dc_sapi,
			log: ctx.ctx_log.child({ component: 'sapi/' +
			    dc.dc_name }),
			agent: false
		});
	});

	setImmediate(done);
}

function
create_remote_cnapi_clients(ctx, done)
{
	mod_jsprim.forEachKey(ctx.ctx_dcs, function (n, dc) {
		dc.dc_cnapi = dc.dc_app.metadata.cnapi_domain;

		ctx.ctx_log.debug({ dc_cnapi_url: dc.dc_cnapi },
		    'found CNAPI URL for DC "%s"', n);

		dc.dc_clients.dcc_cnapi = new mod_sdc.CNAPI({
			url: 'http://' + dc.dc_cnapi,
			log: ctx.ctx_log.child({ component: 'cnapi/' +
			    dc.dc_name }),
			agent: false
		});
	});

	setImmediate(done);
}

function
create_remote_vmapi_clients(ctx, done)
{
	mod_jsprim.forEachKey(ctx.ctx_dcs, function (n, dc) {
		dc.dc_vmapi = dc.dc_app.metadata.vmapi_domain;

		ctx.ctx_log.debug({ dc_vmapi_url: dc.dc_vmapi },
		    'found VMAPI URL for DC "%s"', n);

		dc.dc_clients.dcc_vmapi = new mod_sdc.VMAPI({
			url: 'http://' + dc.dc_vmapi,
			log: ctx.ctx_log.child({ component: 'vmapi/' +
			    dc.dc_name }),
			agent: false
		});
	});

	setImmediate(done);
}

function
create_remote_ur_clients(ctx, done)
{
	mod_vasync.forEachPipeline({ inputs: Object.keys(ctx.ctx_dcs),
	    func: function (n, next) {
		var dc = ctx.ctx_dcs[n];

		if (dc.dc_clients.dcc_urclient) {
			setImmediate(next);
			return;
		}

		mod_assert.string(dc.dc_app.metadata.rabbitmq, 'rabbitmq');
		var t = dc.dc_app.metadata.rabbitmq.split(':');
		mod_assert.equal(t.length, 4, 'rabbitmq config malformed');

		ctx.ctx_log.info('connecting to ur for DC "%s"', n);
		var urc = mod_urclient.create_ur_client({
			consumer_name: 'manta_reshard',
			reconnect: true,
			amqp_config: {
				login: t[0],
				password: t[1],
				host: t[2],
				port: Number(t[3])
			},
			log: ctx.ctx_log.child({ component: 'dc/' + n }),
			connect_timeout: 30 * 1000,
			enable_http: false,
		});

		urc.once('ready', function () {
			ctx.ctx_log.info('ur connected for DC "%s"', n);
			dc.dc_clients.dcc_urclient = urc;
			next();
		});

	}}, function (err) {
		if (err) {
			ctx.ctx_log.warn(err, 'could not make ur clients');
			retry(create_remote_ur_clients, ctx, done, 5);
			return;
		}

		setImmediate(done);
	});
}

function
load_remote_sdc_applications(ctx, done)
{
	mod_vasync.forEachPipeline({ inputs: Object.keys(ctx.ctx_dcs),
	    func: function (n, next) {
		var dc = ctx.ctx_dcs[n];

		get_sapi_application(ctx.ctx_log, dc.dc_clients.dcc_sapi,
		    'sdc', false, function (err, app) {
			if (err) {
				next(new VE(err, 'remote DC "%s"', n));
				return;
			}

			dc.dc_app = app;

			ctx.ctx_log.info('found "sdc" app %s in DC "%s"',
			    dc.dc_app.uuid, n);

			next();
		});

	} }, function (err) {
		if (err) {
			ctx.ctx_log.warn(err, 'could not load remote ' +
			    '"sdc" SAPI applications (retrying)');
			retry(load_remote_sdc_applications, ctx, done, 5);
			return;
		}

		setImmediate(done);
	});
}


(function
main()
{
	var ctx = {
		ctx_cfgfile: mod_path.join(__dirname, '..', 'etc',
		    'config.json'),
		ctx_cfgfile_notfound: 0
	};

	var log = ctx.ctx_log = mod_bunyan.createLogger({
		name: 'reshard',
		level: process.env.LOG_LEVEL || mod_bunyan.DEBUG,
		serializers: mod_bunyan.stdSerializers
	});

	log.info('starting up');
	mod_vasync.pipeline({ arg: ctx, funcs: [
		/*
		 * Load the configuration file from disk as soon as it
		 * exists.
		 */
		load_config,

		/*
		 * To complete our initial configuration, we need a UFDS and
		 * SAPI client for the local datacentre.
		 */
		create_ufds_client,
		create_sapi_client,

		/*
		 * Get the list of datacentres in this region from UFDS.  We
		 * use this to determine if this is a single- or multi-DC
		 * deployment, as well as to find the full list of DCs for
		 * which we need SAPI clients.
		 */
		find_datacentres,

		/*
		 * Load the full set of SAPI instances for Manta.  We coalesce
		 * the metadata for each instance to collect the full set of
		 * SAPI URLs for all DCs.
		 */
		load_manta_application,
		load_manta_objects,
		find_sapi_urls,

		/*
		 * We need a SAPI client for each remote DC to enable us to
		 * look at configuration for that DC and to discover the
		 * service names of various Triton APIs.  To make sure the SAPI
		 * client works, we also load a copy of the "sdc" SAPI
		 * application from each DC.
		 */
		create_remote_sapi_clients,
		load_remote_sdc_applications,

		/*
		 * Once we have loaded the "sdc" application from each remote
		 * DC, we can create clients for other APIs (e.g., CNAPI).
		 */
		create_remote_cnapi_clients,
		create_remote_vmapi_clients,
		create_remote_ur_clients,

		/*
		 * Our state is tracked in the administrative Moray shard.
		 */
		lib_data_access.create_moray_client,

		lib_http_server.create_http_server,

		lib_executor.create_executor,

	] }, function (err) {
		if (err) {
			log.fatal(err, 'startup failure');
			process.exit(1);
		}

		log.info('startup complete');
	});
})();
