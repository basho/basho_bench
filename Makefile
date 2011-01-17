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
