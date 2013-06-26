
-module(basho_bench_driver_leveldb).

-record(state, {ref, id}).

-export([new/1,
         run/4]).

%% ====================================================================
%% API
%% ====================================================================

new(1) ->
    application:load(eleveldb),
    Config = basho_bench_config:get(eleveldb, [{max_open_files, 50}]),
    [ok = application:set_env(leveldb, K, V) || {K, V} <- Config],
    {ok, _} = eleveldb_sup:start_link(),
    setup(1);
new(Id) ->
    setup(Id).

setup(Id) ->
    Config = basho_bench_config:get(eleveldb, [{max_open_files, 50}]),
    WorkDir = basho_bench_config:get(leveldb_work_dir, "/tmp/leveldb"),
    case eleveldb_conn:is_open() of
        false ->
            case eleveldb_conn:open(WorkDir, [{create_if_missing, true}] ++ Config) of
                {ok, Ref} ->
                    {ok, #state{ ref=Ref, id=Id }};
                {error, _}=E ->
                    E
            end;
        true ->
            {ok, Ref} = eleveldb_conn:get(),
            {ok, #state{ ref=Ref, id=Id }}
    end.

run(get, KeyGen, _ValueGen, State) ->
    print_status(State, 100000),
    Key = iolist_to_binary(KeyGen()),
    case eleveldb:get(State#state.ref, Key, []) of
        {ok, _Value} ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    print_status(State, 100000),
    Key = iolist_to_binary(KeyGen()),
    case eleveldb:put(State#state.ref, Key, ValueGen(), []) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, State, Reason}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    print_status(State, 100000),
    Key = iolist_to_binary(KeyGen()),
    case eleveldb:delete(State#state.ref, Key, []) of
        ok ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, State, Reason}
    end.


print_status(#state{ref=Ref, id=1}, Count) ->
    status_counter(Count, fun() ->
                               {ok, S} = eleveldb:status(Ref, <<"leveldb.stats">>),
                               io:format("~s\n", [S])
                          end);

print_status(_State, _Count) ->
    ok.


status_counter(Max, Fun) ->
    Curr = case erlang:get(status_counter) of
               undefined ->
                   -1;
               Value ->
                   Value
           end,
    Next = (Curr + 1) rem Max,
    erlang:put(status_counter, Next),
    case Next of
        0 -> Fun(), ok;
        _ -> ok
    end.
