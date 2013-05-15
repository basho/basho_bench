REPO            ?= basho_bench

PKG_REVISION    ?= $(shell git describe --tags)
PKG_VERSION     ?= $(shell git describe --tags | tr - .)
PKG_ID           = bashobench-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar
OVERLAY_VARS    ?=


.PHONY: deps

all: deps compile
	./rebar skip_deps=true escriptize

deps:
	./rebar get-deps

compile: deps
	./rebar compile

clean:
	@./rebar clean

distclean: clean
	@rm -rf basho_bench deps

results:
	priv/summary.r -i tests/current

byte_sec-results:
	priv/summary.r --ylabel1stgraph byte/sec -i tests/current

kbyte_sec-results:
	priv/summary.r --ylabel1stgraph Kbyte/sec -i tests/current

mbyte_sec-results:
	priv/summary.r --ylabel1stgraph Mbyte/sec -i tests/current

##
## Packaging targets
##
.PHONY: package
export PKG_VERSION PKG_ID PKG_BUILD BASE_DIR ERLANG_BIN REBAR OVERLAY_VARS RELEASE

package.src: deps
	mkdir -p package
	rm -rf package/$(PKG_ID)
	git archive --format=tar --prefix=$(PKG_ID)/ $(PKG_REVISION)| (cd package && tar -xf -)
	${MAKE} -C package/$(PKG_ID) deps
	for dep in package/$(PKG_ID)/deps/*; do \
 	    echo "Processing dep: $${dep}"; \
	    mkdir -p $${dep}/priv; \
	    git --git-dir=$${dep}/.git describe --tags >$${dep}/priv/vsn.git; \
        done
	find package/$(PKG_ID) -depth -name ".git" -exec rm -rf {} \;
	tar -C package -czf package/$(PKG_ID).tar.gz $(PKG_ID)

dist: package.src
	cp package/$(PKG_ID).tar.gz .

package: package.src
	${MAKE} -C package -f $(PKG_ID)/deps/node_package/Makefile

pkgclean: distclean
	rm -rf package
