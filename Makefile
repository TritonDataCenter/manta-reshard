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

NAME :=				manta-reshard

NODE_PREBUILT_TAG =		gz
NODE_PREBUILT_VERSION =		v4.8.7
NODE_PREBUILT_IMAGE =		18b094b0-eb01-11e5-80c1-175dac7ddf02

NODE_DEV_SYMLINK =		node

PROTO =				proto
PREFIX =			/opt/smartdc/$(NAME)

CLEAN_FILES +=			$(PROTO)

RELEASE_TARBALL =		$(NAME)-pkg-$(STAMP).tar.bz2


include ./tools/mk/Makefile.defs
include ./tools/mk/Makefile.node_prebuilt.defs
include ./tools/mk/Makefile.node_modules.defs

.PHONY: all
all: $(STAMP_NODE_PREBUILT) $(STAMP_NODE_MODULES)
	$(NODE) --version

#
# Install macros and targets:
#

COMMANDS =			$(subst .js,,$(notdir $(wildcard cmd/*.js)))

LIB_FILES =			$(notdir $(wildcard lib/*.js))

SCRIPTS =			firstboot.sh \
				everyboot.sh \
				backup.sh \
				services.sh \
				util.sh
SCRIPTS_DIR =			$(PREFIX)/scripts

TEMPLATES =			$(notdir $(wildcard templates/*))
TEMPLATES_DIR =			$(PREFIX)/templates

BOOT_SCRIPTS =			setup.sh configure.sh
BOOT_DIR =			/opt/smartdc/boot

SAPI_MANIFESTS =		manta-reshard
SAPI_MANIFEST_DIRS =		$(SAPI_MANIFESTS:%=$(PREFIX)/sapi_manifests/%)

SMF_MANIFESTS =			reshard
SMF_MANIFESTS_DIR =		$(PREFIX)/smf/manifests

NODE_BITS =			bin/node \
				lib/libgcc_s.so.1 \
				lib/libstdc++.so.6
NODE_DIR =			$(PREFIX)/node
NODE_MODULE_INSTALL =		$(PREFIX)/node_modules/.ok

INSTALL_FILES =			$(addprefix $(PROTO), \
				$(BOOT_SCRIPTS:%=$(BOOT_DIR)/%) \
				$(SCRIPTS:%=$(SCRIPTS_DIR)/%) \
				$(TEMPLATES:%=$(TEMPLATES_DIR)/%) \
				$(SMF_MANIFESTS:%=$(SMF_MANIFESTS_DIR)/%.xml) \
				$(NODE_BITS:%=$(NODE_DIR)/%) \
				$(NODE_MODULE_INSTALL) \
				$(COMMANDS:%=$(PREFIX)/cmd/%.js) \
				$(COMMANDS:%=$(PREFIX)/bin/%) \
				$(LIB_FILES:%=$(PREFIX)/lib/%) \
				$(PREFIX)/lib/wrap.sh \
				$(SAPI_MANIFEST_DIRS:%=%/template) \
				$(SAPI_MANIFEST_DIRS:%=%/manifest.json) \
				)

INSTALL_DIRS =			$(addprefix $(PROTO), \
				$(SCRIPTS_DIR) \
				$(TEMPLATES_DIR) \
				$(SMF_MANIFESTS_DIR) \
				$(BOOT_DIR) \
				$(NODE_DIR)/bin \
				$(NODE_DIR)/lib \
				$(PREFIX)/cmd \
				$(PREFIX)/bin \
				$(PREFIX)/lib \
				$(SAPI_MANIFEST_DIRS) \
				)

INSTALL_EXEC =			rm -f $@ && cp $< $@ && chmod 755 $@
INSTALL_FILE =			rm -f $@ && cp $< $@ && chmod 644 $@


.PHONY: install
install: $(INSTALL_FILES)

$(INSTALL_DIRS):
	mkdir -p $@

$(PROTO)$(BOOT_DIR)/setup.sh: | $(INSTALL_DIRS)
	rm -f $@ && ln -s ../$(NAME)/scripts/firstboot.sh $@

$(PROTO)$(BOOT_DIR)/configure.sh: | $(INSTALL_DIRS)
	rm -f $@ && ln -s ../$(NAME)/scripts/everyboot.sh $@

$(PROTO)$(PREFIX)/scripts/%.sh: deps/manta-scripts/%.sh | $(INSTALL_DIRS)
	$(INSTALL_EXEC)

$(PROTO)$(PREFIX)/scripts/%.sh: boot/%.sh | $(INSTALL_DIRS)
	$(INSTALL_EXEC)

$(PROTO)$(PREFIX)/templates/%: templates/% | $(INSTALL_DIRS)
	$(INSTALL_FILE)

$(PROTO)$(PREFIX)/node/bin/%: $(STAMP_NODE_PREBUILT) | $(INSTALL_DIRS)
	rm -f $@ && cp $(NODE_INSTALL)/node/bin/$(@F) $@ && chmod 755 $@

$(PROTO)$(PREFIX)/node/lib/%: $(STAMP_NODE_PREBUILT) | $(INSTALL_DIRS)
	rm -f $@ && cp $(NODE_INSTALL)/node/lib/$(@F) $@ && chmod 755 $@

$(PROTO)$(PREFIX)/cmd/%.js: cmd/%.js | $(INSTALL_DIRS)
	$(INSTALL_FILE)

$(PROTO)$(PREFIX)/bin/%:
	rm -f $@ && ln -s ../lib/wrap.sh $@

$(PROTO)$(PREFIX)/lib/%.sh: lib/%.sh | $(INSTALL_DIRS)
	$(INSTALL_EXEC)

$(PROTO)$(PREFIX)/lib/%.js: lib/%.js | $(INSTALL_DIRS)
	$(INSTALL_FILE)

$(PROTO)$(NODE_MODULE_INSTALL): $(STAMP_NODE_MODULES) | $(INSTALL_DIRS)
	rm -rf $(@D)/
	cp -rP node_modules/ $(@D)/
	touch $@

$(PROTO)$(PREFIX)/sapi_manifests/%: sapi_manifests/% | $(INSTALL_DIRS)
	$(INSTALL_FILE)

$(PROTO)$(PREFIX)/smf/manifests/%.xml: smf/manifests/%.xml | $(INSTALL_DIRS)
	$(INSTALL_FILE)

#
# Mountain Gorilla targets:
#

.PHONY: release
release: install
	@echo "==> Building $(RELEASE_TARBALL)"
	cd $(PROTO) && gtar -jcf $(TOP)/$(RELEASE_TARBALL) \
	    --transform='s,^[^.],root/&,' \
	    --owner=0 --group=0 \
	    opt

.PHONY: publish
publish: release
	@if [[ -z "$(BITS_DIR)" ]]; then \
		echo "error: 'BITS_DIR' must be set for 'publish' target"; \
		exit 1; \
	fi
	mkdir -p $(BITS_DIR)/$(NAME)
	cp $(RELEASE_TARBALL) $(BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)


include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.targ
include ./tools/mk/Makefile.node_prebuilt.targ
include ./tools/mk/Makefile.node_modules.targ
