
all:
	./rebar get-deps compile test escriptize

clean:
	@./rebar clean

distclean: clean
	@rm -rf basho_bench deps

results:
	priv/summary.r -i tests/current
