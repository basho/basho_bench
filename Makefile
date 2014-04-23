.PHONY: deps

all: deps compile
	./rebar skip_deps=true escriptize

deps:
	./rebar get-deps

compile: deps
	@(./rebar compile)

clean:
	@./rebar clean

distclean: clean
	@rm -rf basho_bench deps

results:
	Rscript --vanilla priv/summary.r -i tests/current

byte_sec-results:
	Rscript --vanilla priv/summary.r --ylabel1stgraph byte/sec -i tests/current

kbyte_sec-results:
	Rscript --vanilla priv/summary.r --ylabel1stgraph Kbyte/sec -i tests/current

mbyte_sec-results:
	Rscript --vanilla priv/summary.r --ylabel1stgraph Mbyte/sec -i tests/current

TARGETS := $(shell ls tests/ | grep -v current)
JOBS := $(addprefix job,${TARGETS})
.PHONY: all_results ${JOBS}

all_results: ${JOBS} ; echo "$@ successfully generated."
${JOBS}: job%: ; Rscript --vanilla priv/summary.r -i tests/$*
