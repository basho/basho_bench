REPO            ?= basho_bench

PKG_REVISION    ?= $(shell git describe --tags)
PKG_VERSION     ?= $(shell git describe --tags | tr - .)
PKG_ID           = basho-bench-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar
OVERLAY_VARS    ?=


all: deps compile
	$(REBAR) skip_deps=true escriptize

.PHONY: deps compile rel lock locked-all locked-deps

rel: deps compile
	cd rel && .$(REBAR) generate skip_deps=true $(OVERLAY_VARS)

deps:
	$(REBAR) get-deps

##
## Lock Targets
##
##  see https://github.com/seth/rebar_lock_deps_plugin
lock: deps compile
	$(REBAR) lock-deps

locked-all: locked-deps compile

locked-deps:
	@echo "Using rebar.config.lock file to fetch dependencies"
	$(REBAR) -C rebar.config.lock get-deps

compile: deps
	# Temp hack to work around https://github.com/basho/riak-erlang-client/issues/151
	(cd deps/riak_pb ; $(REBAR) clean compile deps_dir=..)
	@($(REBAR) compile)

clean:
	@$(REBAR) clean

distclean: clean
	@rm -rf basho_bench deps

results:
	Rscript --vanilla priv/summary.r -i tests/current

ops_sec-results: results

byte_sec-results:
	Rscript --vanilla priv/summary.r --ylabel1stgraph byte/sec -i tests/current

kb_sec-results:
	Rscript --vanilla priv/summary.r --ylabel1stgraph KB/sec -i tests/current

kib_sec-results:
	Rscript --vanilla priv/summary.r --ylabel1stgraph KiB/sec -i tests/current

mb_sec-results:
	Rscript --vanilla priv/summary.r --ylabel1stgraph MB/sec -i tests/current

mib_sec-results:
	Rscript --vanilla priv/summary.r --ylabel1stgraph MiB/sec -i tests/current

results-browser:
	cp -R priv/results-browser/* tests/current && cd tests/current && python -c 'import os, json; print json.dumps(os.listdir("."))' > web/data.json && python ../../priv/results-browser.py

TARGETS := $(shell ls tests/ | grep -v current)
JOBS := $(addprefix job,${TARGETS})
.PHONY: all_results ${JOBS}

all_results: ${JOBS} ; echo "$@ successfully generated."
${JOBS}: job%: ; Rscript --vanilla priv/summary.r -i tests/$*

##
## Packaging targets
##
.PHONY: package
export PKG_VERSION PKG_ID PKG_BUILD BASE_DIR ERLANG_BIN REBAR OVERLAY_VARS RELEASE

package.src: deps
	mkdir -p package
	rm -rf package/$(PKG_ID)
	git archive --format=tar --prefix=$(PKG_ID)/ $(PKG_REVISION)| (cd package && tar -xf -)
	${MAKE} -C package/$(PKG_ID) locked-deps
	for dep in package/$(PKG_ID)/deps/*; do \
	    echo "Processing dep: $${dep}"; \
	    mkdir -p $${dep}/priv; \
	    git --git-dir=$${dep}/.git describe --always --tags >$${dep}/priv/vsn.git; \
        done
	find package/$(PKG_ID) -depth -name ".git" -exec rm -rf {} \;
	tar -C package -czf package/$(PKG_ID).tar.gz $(PKG_ID)

dist: package.src
	cp package/$(PKG_ID).tar.gz .

package: package.src
	${MAKE} -C package -f $(PKG_ID)/deps/node_package/Makefile

pkgclean: distclean
	rm -rf package
