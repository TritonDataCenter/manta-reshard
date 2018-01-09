
var mod_verror = require('verror');
var mod_jsprim = require('jsprim');
var mod_assert = require('assert-plus');

var lib_http_client = require('../lib/http_client');

var VE = mod_verror.VError;

function
manatee_restore_watch(opts)
{
	mod_assert.object(opts, 'opts');
	mod_assert.string(opts.ip, 'ip');
	mod_assert.number(opts.port, 'port');
	mod_assert.object(opts.status, 'status');

	var active = true;
	var timeo;
	var seen_uuids = {};
	var status = opts.status;

	var resched = function () {
		clearTimeout(timeo);
		if (active) {
			timeo = setTimeout(fetch, 1000);
		}
		return (active);
	};

	var fetch = function () {
		lib_http_client.http_get(opts.ip, opts.port, '/restore',
		    function (err, obj) {
			if (!resched()) {
				return;
			}

			status.clear();

			if (err) {
				status.prop('restore status error',
				    err.message);
				return;
			}

			var r = obj.restore;
			if (r === null) {
				status.prop('restore', 'none in progress');
				return;
			}

			seen_uuids[r.uuid] = true;
			if (Object.keys(seen_uuids).length > 1) {
				status.prop('warning', 'restore job may have ' +
				    'restarted at least ' +
				    (Object.keys(seen_uuids).length - 1) +
				    ' times');
			}

			var c = Number(r.completed || 0) / 1024 / 1024;
			var s = Number(r.size) / 1024 / 1024;

			var cc = Math.round(c);
			var ss = Math.round(s);

			if (r.done) {
				status.prop('restore', 'complete (' +
				    cc + ' MB)');
				return;
			}

			var p = Math.round(100 * c / s);
			status.prop('restore',  cc + '/' + ss + ' MB (' +
			    p + '%)');
		});
	};

	resched();

	var obj = {
		cancel: function () {
			active = false;
			clearTimeout(timeo);
		},
	};

	return (obj);
}

function
generate_cluster_state(opts)
{
	mod_assert.object(opts, 'opts');
	mod_assert.arrayOfObject(opts.peers, 'opts.peers');
	mod_assert.strictEqual(opts.peers.length, 3, 'must be three peers');
	mod_assert.string(opts.init_wal, 'opts.init_wal');
	mod_assert.uuid(opts.plan_uuid, 'opts.plan_uuid');

	var pg_url = function (idx) {
		mod_assert.string(opts.peers[idx].ip, 'ip');

		return ('tcp://postgres@' + opts.peers[idx].ip +
		    ':5432/postgres');
	};

	var backup_url = function (idx) {
		mod_assert.string(opts.peers[idx].ip, 'ip');

		return ('http://' + opts.peers[idx].ip + ':12345');
	};

	var peer = function (idx) {
		mod_assert.string(opts.peers[idx].ip, 'ip');
		mod_assert.uuid(opts.peers[idx].zone, 'zone');

		return ({
			id: opts.peers[idx].ip + ':5432:12345',
			ip: opts.peers[idx].ip,
			pgUrl: pg_url(idx),
			zoneId: opts.peers[idx].zone,
			backupUrl: backup_url(idx),
		});
	};

	var state = {
		generation: 1,
		primary: peer(0),
		sync: peer(1),
		async: [
			peer(2)
		],
		deposed: [],
		initWal: opts.init_wal,
		freeze: {
			date: (new Date()).toISOString(),
			reason: 'reshard plan ' + opts.plan_uuid
		}
	};

	return (state);
}

/*
 * Accepts the "stdout" output from "manatee-adm show -v" and parses it into an
 * object.  If the input is not in the expected form, an Error is returned.
 */
function
parse_manatee_adm_show(stdout)
{
	var lines = stdout.split('\n');
	var pos = 0;
	var props = {};
	var peers = [];
	var issues = [];

	mod_assert.strictEqual(lines.pop(), '', 'no terminating line feed');

	var fail_invalid = function () {
		return (new VE({ info: { stdout: stdout, line: lines[pos] }},
		    'invalid line (%d) in "manatee-adm show -v" output: "%s"',
		    pos + 1, lines[pos]));
	};

	var unexpected_end = function () {
		return (new VE({ info: { stdout: stdout }},
		    'unexpected end of "manatee-adm show -v" output'));
	};

	var lookup_role = function (role) {
		return (peers.filter(function (peer) {
			return (peer.role === role);
		}));
	};

	var lookup_peer = function (id) {
		var res = peers.filter(function (peer) {
			if (id.length === 8) {
				return (peer.uuid.substr(0, 8) === id);
			} else {
				return (peer.uuid === id);
			}
		});

		if (res.length === 0) {
			return (null);
		}

		mod_assert.strictEqual(res.length, 1, 'duplicate/ambiguous ' +
		    'peer ' + id);

		return (res[0]);
	};

	var add_peer = function (peer) {
		mod_assert.uuid(peer.uuid, 'peer.uuid');

		/*
		 * First, check that we are not going to add a duplicate or
		 * ambiguous entry to the list.
		 */
		var p;
		if ((p = lookup_peer(peer.uuid)) !== null ||
		    (p = lookup_peer(peer.uuid.substr(0, 8))) !== null) {
			return (new VE('duplicate/ambiguous peer: %s',
			    p.uuid));
		}

		if ((peer.role === 'primary' || peer.role === 'sync') &&
		    (p = lookup_role(peer.role)).length !== 0) {
			return (new VE('duplicate peers for role %s: %j',
			    peer.role, p.concat([ peer ])));
		}

		peer.index = peers.length;
		peers.push(peer);
		return (null);
	};

	/*
	 * The first stanza is a set of key-value pairs, with the key first,
	 * then a colon, then a variable amount of whitespace, then the value.
	 * This stanza is terminated by a blank line.
	 */
	for (;;) {
		if (pos >= lines.length) {
			return (unexpected_end());
		}

		var l = lines[pos];

		if (l === '') {
			pos++;
			break;
		}

		var m = l.match(/^([^:]+):\s+(.*)$/);
		if (!m) {
			return (fail_invalid());
		}

		var key = m[1].replace(/[-\s]/g, '_');

		props[key] = m[2];

		pos++;
	}

	/*
	 * The next stanza is the list of peers (including full zone name) and
	 * IP address.  There is a header row, then a set of peers, then a
	 * blank line.
	 */
	var headings = lines[pos].trimRight().split(/\s+/);
	if (!mod_jsprim.deepEqual(headings, [ 'ROLE', 'PEERNAME', 'IP' ])) {
		return (fail_invalid());
	}
	pos++;

	for (;;) {
		if (pos >= lines.length) {
			return (unexpected_end());
		}

		var l = lines[pos];

		if (l === '') {
			pos++;
			break;
		}

		var m = l.match(/^([^\s]+)\s+([a-f0-9-]{36})\s+([^\s]+)\s*$/);
		if (!m) {
			return (fail_invalid());
		}

		var err = add_peer({ uuid: m[2], role: m[1], ip: m[3] });
		if (err) {
			return (err);
		}

		pos++;
	}

	/*
	 * The penultimate stanza lists each peer with its current replication
	 * status.  There is a header row, then a set of peers.
	 */
	var headings = lines[pos].trimRight().split(/\s+/);
	if (!mod_jsprim.deepEqual(headings, [ 'ROLE', 'PEER', 'PG', 'REPL',
	    'SENT', 'FLUSH', 'REPLAY', 'LAG' ])) {
		return (fail_invalid());
	}
	pos++;

	var expect_issues = false;
	var lastidx = -1;
	var nextrepl = null;
	for (;;) {
		if (pos >= lines.length) {
			break;
		}

		var l = lines[pos];

		if (l === '') {
			/*
			 * If there is a blank separating line, rather than
			 * just the end of the output, we expect some cluster
			 * issue messages to be printed next.
			 */
			expect_issues = true;
			pos++;
			break;
		}

		var t = l.trimRight().split(/\s+/);
		if (t.length !== headings.length) {
			return (fail_invalid());
		}

		var p = lookup_peer(t[1]);
		if (!p || p.index !== lastidx + 1) {
			return (fail_invalid());
		}
		lastidx = p.index;

		p.role = t[0];
		p.pg = t[2];
		p.lag = t[7] !== '-' ? t[7] : null;
		p.sent = t[4] !== '-' ? t[4] : null;

		if (nextrepl !== null) {
			p.repl = nextrepl.repl;
			p.flush = nextrepl.flush;
			p.replay = nextrepl.replay;
			nextrepl = null;
		}

		if (t[3] !== '-') {
			nextrepl = {
				repl: t[3],
				flush: t[5],
				replay: t[6],
			};
		}

		pos++;
	}

	if (nextrepl !== null) {
		return (new VE('extra replication status line: %j', nextrepl));
	}

	if (pos < lines.length) {
		if (!expect_issues) {
			return (fail_invalid());
		}
		lines.slice(pos).forEach(function (l) {
			if (l !== '') {
				issues.push(l);
			}
		});
	} else {
		if (expect_issues) {
			return (new VE('expected issues, but found none'));
		}
	}

	/*
	 * Check to ensure every peer in the full list stanza also appears in
	 * the PostgreSQL status stanza.
	 */
	for (var i = 0; i < peers.length; i++) {
		if (!peers[i].pg) {
			return (new VE('peer %s did not appear in pg-status',
			    peers[i].uuid));
		}
	}

	return ({
		props: props,
		peers: peers,
		issues: issues
	});
}


module.exports = {
	parse_manatee_adm_show: parse_manatee_adm_show,
	generate_cluster_state: generate_cluster_state,
	manatee_restore_watch: manatee_restore_watch,
};
