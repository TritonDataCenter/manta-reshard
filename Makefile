#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2017, Joyent, Inc.
#

#
# Makefile: Manta Resharding System
#

NAME :=				reshard

NODE_PREBUILT_TAG =		gz
NODE_PREBUILT_VERSION =		v4.8.5
NODE_PREBUILT_IMAGE =		18b094b0-eb01-11e5-80c1-175dac7ddf02


include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.node_prebuilt.defs

.PHONY: all
all: $(STAMP_NODE_PREBUILT)
	$(NODE) --version

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.targ
include ./tools/mk/Makefile.node_prebuilt.targ
