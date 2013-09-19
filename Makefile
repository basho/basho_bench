## just for velvet lying under riak_cs_bench to support both R15B0x and R16B01
## wrappers are in velvet codes, but might be useful for other clients around.
VSN := $(shell erl -eval 'io:format("~s~n", [erlang:system_info(otp_release)]), init:stop().' | grep 'R' | sed -e 's,R\(..\)B.*,\1,')
NEW_HASH := $(shell expr $(VSN) \>= 16)

.PHONY: deps

all: deps compile
	./rebar skip_deps=true escriptize

deps:
	./rebar get-deps

compile: deps
ifeq ($(NEW_HASH),1)
	@(./rebar compile -Dnew_hash)
else
	@(./rebar compile)
endif


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
