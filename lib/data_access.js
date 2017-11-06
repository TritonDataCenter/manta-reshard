

var mod_assert = require('assert-plus');
var mod_moray = require('moray');
var mod_verror = require('verror');
var mod_jsprim = require('jsprim');

var VE = mod_verror.VError;


function
create_bucket(c, log, callback)
{
	mod_assert.func(callback, 'callback');

	var name = 'manta_reshard';
	var bcfg = {
		options: {
			version: 1
		}
	};

	log.debug({ bucket_config: bcfg }, 'creating bucket "%s"', name);

	c.putBucket(name, bcfg, function (err) {
		if (err) {
			callback(new VE(err, 'creating bucket "%s"', name));
			return;
		}

		log.debug('moray setup complete');

		callback();
	});
}

function
create_moray_client(opts, callback)
{
	mod_assert.object(opts, 'opts');
	mod_assert.object(opts.moray_config, 'opts.moray_config');
	mod_assert.object(opts.log, 'opts.log');
	mod_assert.func(callback, 'callback');

	var cfg = mod_jsprim.deepCopy(opts.moray_config);
	var log = cfg.log = opts.log;

	log.debug({ moray_config: opts.moray_config }, 'connecting to moray');
	var c = mod_moray.createClient(cfg);

	c.once('connect', function () {
		log.debug('connected to moray');
		create_bucket(c, opts.log, callback);
	});
}


module.exports = {
	create_moray_client: create_moray_client
};
