
all:
	./rebar compile test escriptize

clean:
	./rebar clean

results:
	(cd tests/current && R --vanilla < ../../priv/basho_bench.r)
