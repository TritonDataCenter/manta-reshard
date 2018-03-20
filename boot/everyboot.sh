#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

#
# Runs on every boot of a newly reprovisioned "reshard" zone.
# (Installed as "configure.sh" to be executed by the "user-script")
#

printf '==> everyboot @ %s\n' "$(date -u +%FT%TZ)"

exit 0

