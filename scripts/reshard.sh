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

    reshard plan PLAN_UUID tune [TUNING_NAME [TUNING_VALUE]]

        List tuning properties, or set a tuning property.

    reshard status [-r]

        Display status for active plans.  Use "-r" for continuous redraw mode.

    reshard phases

        List all current reshard plan phase names.

EOF
exit 1
