#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2017, Joyent, Inc.
#

printf '==> firstboot @ %s\n' "$(date -u +%FT%TZ)"

set -o xtrace

NAME=manta-reshard

#
# Runs on first boot of a newly reprovisioned "reshard" zone.
# (Installed as "setup.sh" to be executed by the "user-script")
#

SVC_ROOT="/opt/smartdc/$NAME"

if ! source "$SVC_ROOT/scripts/util.sh" ||
    ! source "$SVC_ROOT/scripts/services.sh"; then
	exit 1
fi

export PATH="$SVC_ROOT/bin:$SVC_ROOT/node/bin:/opt/local/bin:/usr/sbin:/bin"

manta_common_presetup

manta_add_manifest_dir "/opt/smartdc/$NAME"

manta_common_setup "$NAME"

manta_common_setup_end

