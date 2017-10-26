#!/bin/bash
# vim: set ts=8 sts=8 sw=8 noet:

set -o errexit
set -o pipefail

DIR=$(cd "$(dirname "$0")/.." && pwd)
NAME=$(basename $0)

NODE="$DIR/node/bin/node"
JS_FILE="$DIR/cmd/$NAME.js"

exec "$NODE" "$JS_FILE" "$@"
