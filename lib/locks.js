/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_assert = require('assert-plus');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var lib_data_access = require('../lib/data_access');

var VE = mod_verror.VError;


function
Locks(ctx)
{
	var self = this;

	mod_assert.object(ctx, 'ctx');

	self.lks_ctx = ctx;
}

/*
 * Each lock is represented by a Moray object which contains the name of the
 * lock, and the identity of the lock holder (if the lock is held).  The lock
 * also contains details about when it was locked and unlocked in the "events"
 * list, which is presently purely for debugging purposes.
 *
 * {
 *	"name": "reshard_critical_section",
 *	"owner": "c0aa1204-e5d7-11e7-89ec-7fe380d05515",
 *	"events": [
 *		{ "t": "create", "time": "2017-..." },
 *		{ "t": "lock", "owner": "...", "time": "2017-..." },
 *		{ "t": "unlock", "owner": "...", "time": "2017-..." },
 *	]
 * }
 */

Locks.prototype.lock = function
lock(name, owner, callback)
{
	var self = this;

	mod_assert.string(name, 'name');
	mod_assert.string(owner, 'owner');
	mod_assert.func(callback, 'callback');

	mod_vasync.waterfall([ function (done) {
		/*
		 * Ensure the lock exists in the database.
		 */
		var dt = (new Date()).toISOString();
		var new_lock = {
			name: name,
			owner: null,
			events: [
				{ t: 'create', time: dt }
			]
		};

		lib_data_access.lock_store(self.lks_ctx, new_lock,
		    function (err, lock) {
			if (err) {
				if (VE.findCauseByName(err,
				    'EtagConflictError')) {
					/*
					 * The lock exists already.
					 */
					done(null, null);
					return;
				}

				done(new VE(err, 'creating lock'));
				return;
			}

			mod_assert.string(lock._etag, 'lock._etag');

			done(null, lock);
		});

	}, function (lock, done) {
		if (lock !== null) {
			/*
			 * We created the lock, so it does not need to be
			 * reloaded from the database.
			 */
			setImmediate(done, null, lock);
			return;
		}

		/*
		 * The lock existed already; load it from the database.
		 */
		lib_data_access.lock_load(self.lks_ctx, name, done);

	}, function (lock, done) {
		mod_assert.string(lock.name, 'lock.name');
		mod_assert.optionalString(lock.owner, 'lock.owner');
		mod_assert.arrayOfObject(lock.events, 'lock.events');
		mod_assert.string(lock._etag, 'lock._etag');

		if (lock.owner === owner) {
			/*
			 * We own the lock already.  There is no need to
			 * update the database.
			 */
			setImmediate(done, null, lock);
			return;
		}

		if (lock.owner !== null) {
			/*
			 * Somebody else owns the lock.
			 */
			var i = { info: { lock_held: true, owner: lock.owner }};
			done(new VE(i, 'lock "%s" owned by "%s"', lock.name,
			    lock.owner));
			return;
		}

		/*
		 * Take the lock by writing to the database.
		 */
		var dt = (new Date()).toISOString();
		mod_assert.strictEqual(lock.owner, null, 'owner must be null');
		lock.owner = owner;
		lock.events.push({ t: 'lock', owner: owner, time: dt });

		lib_data_access.lock_store(self.lks_ctx, lock, done);

	} ], function (err, lock) {
		if (err) {
			callback(new VE(err, 'owner "%s" taking lock "%s"',
			    owner, name));
			return;
		}

		callback(null, lock);
	});
};

Locks.prototype.unlock = function
unlock(name, owner, callback)
{
	var self = this;

	mod_assert.string(name, 'name');
	mod_assert.string(owner, 'owner');
	mod_assert.func(callback, 'callback');

	mod_vasync.waterfall([ function (done) {
		/*
		 * Load the lock from the database.
		 */
		lib_data_access.lock_load(self.lks_ctx, name, done);

	}, function (lock, done) {
		if (lock.owner !== owner) {
			/*
			 * This is very bad.  For some reason, we are
			 * attempting to release a lock that we do not
			 * hold.
			 *
			 * XXX In actual fact, this is not completely true.
			 * It's possible that some fault allowed us to
			 * commit our unlock to the database, but not receive
			 * acknowledgement.  We'll retry the unlock, but then
			 * find we don't hold the lock anymore.
			 */
			callback(new VE({ info: { hold: true }},
			    'lock not held by owner!'));
			return;
		}

		var dt = (new Date()).toISOString();
		lock.events.push({ t: 'unlock', owner: lock.owner,
		    time: dt });
		lock.owner = null;

		lib_data_access.lock_store(self.lks_ctx, lock, done);

	} ], function (err) {
		if (err) {
			callback(new VE(err, 'owner "%s" unlocking "%s"',
			    owner, name));
			return;
		}

		callback();
	});
};


module.exports = {
	Locks: Locks,
};
