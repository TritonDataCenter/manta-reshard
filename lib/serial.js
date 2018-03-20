/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_assert = require('assert-plus');


function
SerialQueue(name)
{
	var self = this;

	mod_assert.string(name, 'name');
	self.sq_name = name;

	self.sq_running = false;
	self.sq_queue = [];
	self.sq_current = null;
}

SerialQueue.prototype._swtch = function
_swtch()
{
	var self = this;

	if (self.sq_running || self.sq_queue.length < 1) {
		/*
		 * We are already running, or we have nothing left to do.
		 */
		return;
	}
	self.sq_running = true;

	var sqe = self.sq_queue.shift();
	mod_assert.func(sqe.sqe_callback, 'sqe_callback');

	var baton = {
		baton_sq: self,
		baton_sqe: sqe,
		baton_time_birth: Date.now(),
		baton_time_release: null,
		baton_released: false,

		release: function () {
			mod_assert.strictEqual(self.sq_current, baton,
			    'mismatched current baton?');
			mod_assert.equal(baton.baton_released, false,
			    'baton released twice?');
			mod_assert.equal(self.sq_running, true,
			    'not running?');

			baton.baton_released = true;
			baton.baton_time_release = Date.now();

			self.sq_current = null;
			self.sq_running = false;
			self._swtch();
		}
	};

	mod_assert.strictEqual(self.sq_current, null, 'sq_current already set');
	self.sq_current = baton;

	setImmediate(sqe.sqe_callback, baton);
};

SerialQueue.prototype.run = function
run(callback)
{
	var self = this;

	self.sq_queue.push({
		sqe_baton: null,
		sqe_when: Date.now(),
		sqe_callback: callback,
	});
	self._swtch();
};


module.exports = {
	SerialQueue: SerialQueue
};
