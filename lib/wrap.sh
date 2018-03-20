#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

set -o errexit
set -o pipefail

DIR=$(cd "$(dirname "$0")/.." && pwd)
NAME=$(basename $0)

NODE="$DIR/node/bin/node"
JS_FILE="$DIR/cmd/$NAME.js"

EXTRA=()
if [[ $NAME == server ]]; then
	EXTRA+=( '--abort-on-uncaught-exception' )
fi

exec "$NODE" "${EXTRA[@]}" "$JS_FILE" "$@"
