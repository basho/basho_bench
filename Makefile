
all:
	./rebar get-deps compile test escriptize

clean:
	./rebar clean

results:
	(cd tests/current && ../../priv/basho_bench.r)
