

var mod_verror = require('verror');

var lib_common = require('../lib/common');
var lib_http_client = require('../lib/http_client');

var VE = mod_verror.VError;


if (process.argv.length !== 3 || !lib_common.is_uuid(process.argv[2])) {
	console.error('ERROR: Usage: unhold <plan_uuid>');
	process.exit(1);
}

lib_http_client.http_post('127.0.0.1', 80,
    '/plan/' + process.argv[2] + '/unhold', function (err, res) {
	if (err) {
		console.error('ERROR: %s', VE.fullStack(err));
		process.exit(1);
	}

	console.log('result: %s', JSON.stringify(res, false, 4));
});
