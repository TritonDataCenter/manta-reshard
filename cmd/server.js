
var mod_bunyan = require('bunyan');

var LOG;

(function
main()
{
	LOG = mod_bunyan.createLogger({
		name: 'reshard',
		level: process.env.LOG_LEVEL || mod_bunyan.DEBUG,
		serializers: mod_bunyan.stdSerializers
	});

	LOG.info('service start');

	/*
	 * For now, hold the process open.
	 */
	setInterval(function () {}, 120 * 1000);
})();
