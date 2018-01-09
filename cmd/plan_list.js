

var mod_verror = require('verror');

var lib_common = require('../lib/common');
var lib_http_client = require('../lib/http_client');

var VE = mod_verror.VError;


//if (process.argv.length !== 2 || !lib_common.is_uuid(process.argv[2])) {
if (process.argv.length !== 2) {
	console.error('ERROR: Usage: list');
	process.exit(1);
}

lib_http_client.http_get('127.0.0.1', 80, '/plans', function (err, res) {
	if (err) {
		lib_http_client.cmd_print_result(err, res);
		return;
	}

	console.log(Object.keys(res.plans).sort().join('\n'));
});
