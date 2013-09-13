-module(basho_bench_driver_ets).

-export([new/1,
         run/4]).

new(_Id) ->
    EtsTable = ets:new(basho_bench, [ordered_set]),
    {ok, EtsTable}.

run(get, KeyGen, _ValueGen, EtsTable) ->
    Start = KeyGen(),
    case ets:lookup(EtsTable, Start) of
        [] -> 
            {ok, EtsTable};
        [{_Key, _Val}] ->
            {ok, EtsTable};
        Error ->
            {error, Error, EtsTable}
    end;

run(put, KeyGen, ValueGen, EtsTable) ->
    Object = {KeyGen(), ValueGen()},
    ets:insert(EtsTable, Object),
    {ok, EtsTable};

run(delete, KeyGen, _ValueGen, EtsTable) ->
    Start = KeyGen(),
    ets:delete(EtsTable, Start),
    {ok, EtsTable}.
    