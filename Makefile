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
	@echo "\n\nPfffft... you wish!  For further details, please spam steve@basho.com\n\n"

byte_sec-results:
	priv/summary.r --ylabel1stgraph byte/sec -i tests/current

kbyte_sec-results:
	priv/summary.r --ylabel1stgraph Kbyte/sec -i tests/current

mbyte_sec-results:
	priv/summary.r --ylabel1stgraph Mbyte/sec -i tests/current
