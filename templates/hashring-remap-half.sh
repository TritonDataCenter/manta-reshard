#!/bin/bash
#
# This script will use the pristine copy of the hash ring database previously
# unpacked in the workspace directory, remapping half of the vnodes for a
# particular pnode so that they now map to a new pnode.  As a sanity check,
# we will also ensure the new pnode does not yet exist.
#

NODE='/opt/smartdc/electric-moray/build/node/bin/node'
EM_BIN='/opt/smartdc/electric-moray/node_modules/.bin'
FASH="$EM_BIN/fash"

export PATH='/usr/bin:/usr/sbin:/sbin'

workspace='/var/tmp/reshard.%%WORKSPACE_ID%%'
shard='tcp://%%SHARD%%:2020'
new_shard='tcp://%%NEW_SHARD%%:2020'

if [[ ! -d "$workspace/hash_ring" ]]; then
	printf 'ERROR: no hash ring in workspace directory: %s\n' \
	    "$workspace" >&2
	exit 1
fi

if ! cd "$workspace"; then
	printf 'ERROR: could not chdir into workspace\n' >&2
	exit 1
fi

#
# These arguments are common to each invocation of the "fash" command,
# directing it to use the LevelDB backend for the hash ring database in
# our workspace directory.
#
FASHARGS=(
	'-b' 'leveldb'
	'-l' "$workspace/hash_ring"
)

#
# Check that the new shard (pnode) does not already exist.
#
if ! pnodes_out=$("$NODE" "$FASH" get-pnodes "${FASHARGS[@]}") ||
    [[ -z $pnodes_out ]]; then
	printf 'ERROR: could not load pnode list\n' >&2
	exit 1
fi
if ! pnodes=$(awk "{ sub(\"^[^']*'\", \"\", \$0);
    sub(\"[ ,'\\\\]]*$\", \"\", \$0);
    printf(\"%s\n\", \$0); }" <<< "$pnodes_out") || [[ -z $pnodes ]]; then
	printf 'ERROR: could not post-process pnode list\n' >&2
	exit 1
fi
while read pnode; do
	if [[ $pnode == $new_shard ]]; then
		printf 'ERROR: shard "%s" already exists\n' "$new_shard" >&2
		exit 1
	fi
done <<< "$pnodes"

#
# Load the full list of vnodes currently mapped to the target shard (pnode).
#
if ! vnodes_out=$("$NODE" "$FASH" get-vnodes "${FASHARGS[@]}" "$shard") ||
    [[ -z $vnodes_out ]]; then
	printf 'ERROR: could not load vnode list\n' >&2
	exit 1
fi

#
# The output format of the vnode list is a bit unfortunate; rather than a
# single vnode appearing on each line, or even being JSON-formatted,
# the output comes from the "util.inspect()" routine.  The output is
# therefore _similar_ to JSON, but not close enough to be useful with
# regular JSON tools.  We will hold our collective noses, and pull it apart
# with "awk".
#
if ! vnodes=$(awk '{ gsub("[^0-9]", "", $0); printf("%d\n", $0); }' \
    <<< "$vnodes_out"); then
	printf 'ERROR: could not post-process vnode list\n' >&2
	exit 1
fi

#
# We want to remap _half_ of the vnodes for the target shard.  Skip every
# second vnode from the list; the skipped vnodes will remain mapped to
# the existing shard.
#
# Batch the linefeed delimited list of entries into 1024 entry comma-separated
# chunks.  By remapping multiple vnodes in a single "fash" invocation, we can
# greatly improve the performance of the entire operation.
#
if ! vnode_batches=$(awk 'NR % 2 != 0 { next; }
    tail > 1024 { tail = 0; printf("\n"); }
    { if (tail) { printf(","); } tail++; printf("%s", $0); }
    END { if (tail) { printf("\n"); } }' <<< "$vnodes"); then
	printf 'ERROR: could not batch vnode list\n' >&2
	exit 1
fi

while read batch; do
	if ! "$NODE" "$FASH" remap-vnode "${FASHARGS[@]}" -v "$batch" \
	    -p "$new_shard" >/dev/null; then
		printf 'ERROR: could not mark vnodes read-only\n' >&2
		exit 1
	fi
done <<< "$vnode_batches"
