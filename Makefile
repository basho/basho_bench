
all:
	./rebar get-deps compile test escriptize

clean:
	./rebar clean

results:
	priv/summary.r -i tests/current
