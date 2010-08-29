.PHONY: deps

all: deps
	./rebar compile eunit escriptize

deps:
	./rebar get-deps

clean:
	@./rebar clean

distclean: clean
	@rm -rf basho_bench deps

results:
	priv/summary.r -i tests/current
