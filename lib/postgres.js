
var mod_assert = require('assert-plus');
var mod_pg = require('pg');
var mod_verror = require('verror');
var mod_jsprim = require('jsprim');

var VE = mod_verror.VError;


function
Postgres(ctl)
{
	var self = this;

	self.pg_log = ctl.log.child({ postgres: true });
	self.pg_info = null;

	self.pg_opts = null;
	self.pg_client = null;
	self.pg_error = null;
	self.pg_active = false;

	self.pg_connect_called = false;
	self.pg_destroy_called = false;
}

Postgres.prototype.connect = function
connect(opts, callback)
{
	mod_assert.object(opts, 'opts');
	mod_assert.func(callback, 'callback');

	var self = this;

	self.pg_opts = mod_jsprim.deepCopy(opts);
	self.pg_opts.database = 'moray';
	self.pg_opts.user = 'moray';
	self.pg_opts.password = 'moray';
	self.pg_opts.port = 5432;

	mod_assert.ok(!self.pg_connect_called, 'connect() called twice');
	self.pg_connect_called = true;

	if (self.pg_destroy_called) {
		callback(new VE('destroy() already called'));
		return;
	}

	var c = new mod_pg.Client(self.pg_opts);

	c.connect(function (err) {
		if (err) {
			callback(new VE(err, 'postgres connect error'));
			return;
		}

		if (self.pg_destroy_called) {
			c.end(function () {});
			callback(new VE('destroy() called'));
			return;
		}

		self.pg_client = c;
		self.pg_active = true;

		var eventcb = function (err) {
			if (!self.pg_active) {
				return;
			}
			self.pg_active = false;

			if (err) {
				/*
				 * Make sure all client resources are released.
				 */
				self.pg_client.end();
			}
		};

		c.on('error', function (err) {
			eventcb(err);
		});

		c.on('end', function () {
			eventcb();
		});

		callback();
	});
};

Postgres.prototype.query = function
query(querycfg, callback)
{
	mod_assert.object(querycfg, 'querycfg');
	mod_assert.string(querycfg.text, 'querycfg.text');
	mod_assert.func(callback, 'callback');

	var self = this;

	if (self.pg_client === null || !self.pg_active) {
		setImmediate(callback, new VE('connection unavailable'));
		return;
	}

	self.pg_client.query(querycfg, function (err, res) {
		if (err) {
			callback(err);
			return;
		}

		callback(null, res);
	});
};

Postgres.prototype.destroy = function
destroy(callback)
{
	mod_assert.func(callback, 'callback');

	var self = this;

	self.pg_destroy_called = true;

	if (self.pg_client === null || !self.pg_active) {
		setImmediate(callback);
		return;
	}

	self.pg_active = false;
	self.pg_client.end(function (err) {
		if (err) {
			self.pg_log.warn(err, 'postgres end() error');
		}

		callback();
	});
};

Postgres.prototype.info = function
info()
{
	var self = this;

	mod_assert.string(self.pg_info, 'info_set() must have been called');

	return (self.pg_info);
};

Postgres.prototype.info_set = function
info_set(info)
{
	mod_assert.string(info, 'info');

	var self = this;

	self.pg_info = info;
};

module.exports = {
	Postgres: Postgres,
};
