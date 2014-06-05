-module(basho_bench_crdt).

-compile(export_all).


%% top level tagging arity
create(Pid, BKey, CRDT) when is_tuple(CRDT) andalso element(1, CRDT) =:= map ->
    Map = create(Pid, BKey, map, CRDT),
    riak_pb_socket:modify_type(Pid, Bucket, Key, [create]);    
create(Pid, BKey, CRDT) when is_tuple(CRDT) andalso element(1, CRDT) =:= sets ->
    Set = create(Pid, BKey, sets, CRDT);
%% nothing can be embedded in a counter, so just do the creation.
create(Pid, {Bucket, Key}, CRDT) when is_integer(CRDT) ->
    riakc_pb_socket:modify_type(Pid, Bucket, Key, {counter, {increment, CRDT}, } 


create(Pid, Mod, CRDT) when is_tuple(CRDT) andalso element(1, CRDT) =:= map ->
    [_,Tag|Inner] = tuple_to_list(CRDT),
    Mod:update({Tag, map},
               fun(Map) ->
                       lists:foldl(fun(I, M) ->
                                           create(M, I)
                                   end,
                                   Map)
               end,
               Pid);
%% binaries must be stored in registers
create(Pid, Mod, {Name, Value}) when is_binary(Value) ->
    {Name, Value}
    Mod:update({Name, register},
                     fun(Map) ->
                             riakc_register:set(Value, Map)
                     end,
                     Pid);
create(Pid, Mod, {Name, Value}) when is_tuple(CRDT) andalso element(1, CRDT) =:= sets->
    Mod:update({Name, Set
        

modify(Pid, {Bucket, Key}, Selector, Value) when is_binary(Selector) ->
    {ok, CRDT} = riakc_pb_socket:get(Pid, Bucket, Key),
    SelectorList = binary:split(Selector, <<".">>),
    NewCRDT = modify_value(CRDT, SelectorList, Value),
    riakc_pb_socket:update_type

