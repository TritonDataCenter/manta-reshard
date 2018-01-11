#!/bin/bash

case "$1" in
plan)
	shift
	uuid=$1

	if [[ -z $uuid ]]; then
		exec "plan_list"

	elif (( ${#uuid} == 36 )); then
		shift

		if [[ -z $1 ]]; then
			exec "plan_get" "$uuid"

		else
			case "$1" in
			unhold|archive|pause|resume|tune)
				cmd="plan_$1"
				shift

				exec "$cmd" "$uuid" "$@"
				;;
			esac
		fi
	fi
	;;

status|phases)
	cmd=$1
	shift

	exec "$cmd" "$@"
	;;

esac

cat - >&2 <<'EOF'
ERROR: usage:

    reshard plan

        List all plans.

    reshard plan PLAN_UUID

        Fetch JSON object describing this plan.
        NOTE: This object does not currently have a stable format.

    reshard plan PLAN_UUID [unhold | archive | pause | resume]

        Clear an error (unhold), mark a completed plan for
        archival (archive), or pause/resume a running plan.

    reshard plan PLAN_UUID pause [ PHASE_NAME | none ]

        Set a deferred pause action.  The plan will pause automatically
        when it reaches the named phase, but before performing any
        actions for that phase.  To clear a deferred pause action,
        specify "none" instead of a phase name.

    reshard plan PLAN_UUID tune [TUNING_NAME [TUNING_VALUE]]

        List tuning properties, or set a tuning property.

    reshard status [-r] [-x] [-U] [PLAN_UUID ...]

        Display status for active plans.  Use "-r" for continuous redraw mode.
        To filter by plan UUID, provide either a full PLAN_UUID or a regular
        expression.  To show only plans which are on hold or retrying due
        to an error condition, use "-x".  To force the use of UTF-8 drawing
        characters, use "-U".

    reshard phases

        List all current reshard plan phase names.

EOF
exit 1
