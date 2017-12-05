#!/bin/bash

NODE='/opt/smartdc/electric-moray/build/node/bin/node'

export PATH='/usr/bin:/usr/sbin:/sbin'

"$NODE" -e '
	var mod_fs = require("fs");

	mod_fs.readFile("/var/tmp/reshard.%%WORKSPACE_ID%%/status.json",
	    { encoding: "utf8" }, function (err, data) {
		if (err) {
			console.error("ERROR: %s", err.message);
			process.exit(1);
		}

		var o;
		try {
			o = JSON.parse(data);
		} catch (ex) {
			console.error("ERROR: %s", ex.message);
			process.exit(1);
		}

		var st;
		try {
			st = mod_fs.statSync("/proc/" + o.pid);
			//process.kill(o.pid, 0);
			o._proc_mtime = st.mtime.toISOString();
		} catch (ex) {
			o._process_missing = ex.message;
		}

		console.log("%j", o);
	});
'
