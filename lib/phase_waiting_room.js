
function
phase_waiting_room(ctl)
{
	var i = 0;
	var again = function () {
		if (i > 5) {
			ctl.finish();
			return;
		}

		ctl.log.info({ count: i++ }, 'waiting room!');
		setTimeout(again, 500);
	};
	setTimeout(again, 500);
}

module.exports = {
	phase_waiting_room: phase_waiting_room,
};
