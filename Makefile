REPO            ?= basho_bench

PKG_REVISION    ?= $(shell git describe --tags)
PKG_VERSION     ?= $(shell git describe --tags | tr - .)
PKG_ID           = basho-bench-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar3
OVERLAY_VARS    ?=


all: compile
	$(REBAR) escriptize

.PHONY: compile rel

rel: compile
	cd rel && $(REBAR) generate $(OVERLAY_VARS)

compile:
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
