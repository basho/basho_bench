-module(voxer_gen1).
-compile(export_all).

inserter_keygen2(Id, InsertsPerSec) ->
    fun() -> do_inserter2(Id, InsertsPerSec) end.

do_inserter2(Id, InsertsPerSec) ->
    Workers = basho_bench_config:get(concurrent),
    {Ts, Remaining} = case erlang:get(insert_ts) of
                          undefined ->
                              {A, B, _} = os:timestamp(),
                              {(A * 1000000) + B, InsertsPerSec - Id};
                          {Ts0, 0} ->
                              {Ts0+1, InsertsPerSec - Id};
                          {Ts0, R0} ->
                              {Ts0, R0}
                      end,
    Suffix = random:uniform(InsertsPerSec * 10000),
    erlang:put(insert_ts, {Ts, Remaining-Workers}),
    [integer_to_list(Ts), $_, integer_to_list(Suffix)].


inserter_keygen(Id, InsertsPerSec) ->
    fun() -> do_inserter(Id, InsertsPerSec) end.

do_inserter(_Id, InsertsPerSec) ->
    {Ts, Remaining} = case erlang:get(insert_ts) of
                          undefined ->
                              {A, B, _} = os:timestamp(),
                              {(A * 1000000) + B, InsertsPerSec};
                          {Ts0, 0} ->
                              {Ts0+1, InsertsPerSec};
                          {Ts0, R0} ->
                              {Ts0, R0}
                      end,
    Suffix = random:uniform(InsertsPerSec * 10000),
    erlang:put(insert_ts, {Ts, Remaining-1}),
    [integer_to_list(Ts), $_, integer_to_list(Suffix)].
