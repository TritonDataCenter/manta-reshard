
var mod_path = require('path');
var mod_fs = require('fs');

var mod_assert = require('assert-plus');
var mod_uuid = require('uuid');
var mod_jsprim = require('jsprim');
var mod_extsprintf = require('extsprintf');
var mod_vasync = require('vasync');

var sprintf = mod_extsprintf.sprintf;

/*
 * XXX This function is copied from "sdc-manta.git".  It should be refactored
 * into a common library for creating the SAPI provisioning request for new
 * Manta zones.
 *
 * XXX I have also pruned out the parts not relevant to "moray" or "postgres"
 * zones.
 */

function
create_instance(ctx, opts, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.object(opts, 'opts');
	mod_assert.object(opts.service, 'opts.service');
	mod_assert.string(opts.service.name, 'opts.service.name');
	mod_assert.uuid(opts.service.uuid, 'opts.service.uuid');
	mod_assert.string(opts.shard, 'opts.shard');
	mod_assert.uuid(opts.server_uuid, 'opts.server_uuid');
	mod_assert.uuid(opts.image_uuid, 'opts.image_uuid');
	mod_assert.optionalObject(opts.tags, 'opts.tags');

	mod_assert.ok(opts.service.name === 'postgres' ||
	    opts.service.name === 'moray', 'service must be "postgres" or ' +
	    '"moray"');


	var log = ctx.ctx_log;

	var inst_uuid = mod_uuid.v4();

	var params = {};

	/*
	 * The root of all service hostnames is formed from the application's
	 * region and DNS domain.
	 */
	mod_assert.string(ctx.ctx_cfg.region, 'cfg.region');
	mod_assert.string(ctx.ctx_cfg.dns_domain, 'cfg.dns_domain');
	var service_root = sprintf('%s.%s', ctx.ctx_cfg.region,
	    ctx.ctx_cfg.dns_domain);
	var service_name = sprintf('%s.%s', opts.service.name, service_root);

	params.alias = service_name + '-' + inst_uuid.substr(0, 8);

	/*
	 * Prefix with the shard for things that are shardable...
	 */
	if ([ 'postgres', 'moray' ].indexOf(opts.service.name) !== -1) {
		params.alias = opts.shard + '.' + params.alias;
	}

	params.tags = {};
	params.tags.manta_role = opts.service.name;

	if (opts.tags) {
		mod_jsprim.forEachKey(opts.tags, function (tag, val) {
			params.tags[tag] = val;
		});
	}

	if (opts.server_uuid) {
		params.server_uuid = opts.server_uuid;
	}

	if (opts.image_uuid) {
		params.image_uuid = opts.image_uuid;
	}

	if (opts.networks) {
		var networks = [];
		opts.networks.forEach(function (token) {
			networks.push({ uuid: token });
		});
		params.networks = networks;
	}

	var metadata = {};
	metadata.DATACENTER = opts.datacenter_name;
	metadata.SERVICE_NAME = service_name;
	metadata.SHARD = opts.shard;

	if (opts.service.name === 'postgres' || opts.service.name === 'moray') {
		metadata.SERVICE_NAME = sprintf('%s.moray.%s',
		    opts.shard, service_root);
	}

	if (opts.service.name === 'postgres') {
		metadata.MANATEE_SHARD_PATH = sprintf('/manatee/%s',
		    metadata.SERVICE_NAME);
	}

	/*
	 * This zone should get its configuration the local (i.e. same
	 * datacenter) SAPI instance, as well as use the local UFDS instance.
	 */
	var sdc_app = ctx.ctx_dcs[opts.datacenter_name].dc_app;
	mod_assert.object(sdc_app, 'sdc_app');
	var sdc_md = sdc_app.metadata;
	mod_assert.object(sdc_md, 'sdc_md');
	mod_assert.string(sdc_md.sapi_domain, 'sdc metadata.sapi_domain');

	metadata['SAPI_URL'] = 'http://' + sdc_md.sapi_domain;
	metadata['UFDS_URL'] = 'ldaps://' + sdc_md.ufds_domain;
	metadata['UFDS_ROOT_DN'] = sdc_md.ufds_ldap_root_dn;
	metadata['UFDS_ROOT_PW'] = sdc_md.ufds_ldap_root_pw;
	metadata['SDC_NAMESERVERS'] = sdc_md.ZK_SERVERS;

	mod_vasync.waterfall([ function (subcb) {
		log.info('locating user script');

		var file = mod_path.resolve(mod_path.join(__dirname, '..',
		    'templates', 'user-script.sh'));

		mod_fs.readFile(file, 'ascii', function (err, contents) {
			if (err) {
				log.error(err, 'failed to read user script');
				subcb(err);
				return;
			}

			metadata['user-script'] = contents;
			log.debug('read user script from "%s"', file);
			subcb(null);
		});

	}, function (subcb) {
		var reqopts = {};
		reqopts.params = params;
		reqopts.metadata = metadata;
		reqopts.uuid = inst_uuid;
		reqopts.master = true;

		log.info({ opts: reqopts }, 'creating instance');

		/*
		 * Use the SAPI client for the DC in which we are attempting
		 * to provision.
		 */
		var sapi = ctx.ctx_dcs[opts.datacenter_name].dc_clients.
		    dcc_sapi;

		sapi.createInstance(opts.service.uuid, reqopts,
		    function (err, inst) {
			if (err) {
				log.error(err, 'failed to create ' +
				    'instance');
				subcb(err);
				return;
			}

			log.info({ inst: inst }, 'created instance');

			subcb(null, inst);
		});

	} ], function (err, inst) {
		callback(err, inst);
	});
}


module.exports = {
	create_instance: create_instance,
};
