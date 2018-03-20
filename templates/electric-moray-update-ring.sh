#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

NODE='/opt/smartdc/electric-moray/build/node/bin/node'
EM_BIN='/opt/smartdc/electric-moray/node_modules/.bin'
SDC_IMGADM="$EM_BIN/sdc-imgadm"
export SDC_IMGADM_URL='%%HASH_RING_IMGAPI_SERVICE%%'
HASH_RING_IMAGE='%%HASH_RING_IMAGE%%'

export PATH='/usr/bin:/usr/sbin:/sbin'

fmri='%%FMRI%%'

#
# Determine the location of the hash ring for this Electric Moray instance.
#
if ! hashdir=$(svcprop -p 'electric-moray/ring-location' "$fmri") ||
    [[ -z $hashdir ]]; then
	printf 'ERROR: could not locate hash ring for instance "%s"\n' \
	    "$fmri" >&2
	exit 1
fi

#
# Create a temporary directory into which to download the hash ring from
# IMGAPI.
#
if ! tmpdir=$(mktemp -d); then
	printf 'ERROR: could not create temporary directory\n' >&2
	exit 1
fi

#
# Obtain the current hash ring from IMGAPI.
#
if ! "$NODE" "$SDC_IMGADM" get-file -q -o "$tmpdir/hash_ring.tar.gz" \
    "$HASH_RING_IMAGE" >/dev/null 2>&1; then
	rm -rf "$tmpdir"
	printf 'ERROR: could not download hash ring image "%s" from IMGAPI\n' \
	    "$HASH_RING_IMAGE" >&2
	exit 1
fi

#
# Disable the instance so that we can replace the hash ring.
#
if ! svcadm disable -s "$fmri"; then
	rm -rf "$tmpdir"
	printf 'ERROR: could not disable Electric Moray instance "%s"\n' \
	    "$fmri" >&2
	exit 1
fi

#
# Extract the new hash ring in a directory next to the original one.  Once this
# completes successfully, we will swap it in atomically.  This should ensure
# that we can use the stamp file (contained in the tar file) to determine the
# version without mistakenly accepting a partial extraction.
#
if ! rm -rf "$hashdir.new" || ! mkdir "$hashdir.new" ||
    ! cd "$hashdir.new"; then
	rm -rf "tmpdir"
	printf 'ERROR: could not create hash extraction dir "%s"\n' \
	    "$hashdir.new" >&2
	exit 1
fi

if ! gtar --strip-components=1 -x -f "$tmpdir/hash_ring.tar.gz" \
    'hash_ring'; then
	rm -rf "$tmpdir"
	printf 'ERROR: could not extract hash ring\n' >&2
	exit 1
fi

#
# Clean up the temporary directory and chdir out of the directory we are
# about to shuffle around.
#
rm -rf "$tmpdir"
if ! cd /; then
	printf 'ERROR: could not chdir out of hash ring dir\n' >&2
	exit 1
fi

#
# Remove the existing hash ring and move the new one into place.
#
if ! rm -rf "$hashdir.old"; then
	printf 'ERROR: could not remove vestigial old hash dir\n' >&2
	exit 1
fi
if [[ -d "$hashdir" ]]; then
	if ! mv "$hashdir" "$hashdir.old" || ! rm -rf "$hashdir.old"; then
		printf 'ERROR: could not remove old hash dir\n' >&2
		exit 1
	fi
fi
if ! mv "$hashdir.new" "$hashdir"; then
	printf 'ERROR: could not move new hash dir into place\n' >&2
	exit 1
fi

#
# Re-enable Electric Moray.
#
if ! svcadm enable -s "$fmri"; then
	rm -rf "$tmpdir"
	printf 'ERROR: could not enable Electric Moray instance "%s"\n' \
	    "$fmri" >&2
	exit 1
fi

exit 0
