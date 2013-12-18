-module(basho_bench_valgen_json).

-export([new/2]).

-include("basho_bench.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MAX_VALUE, 999999).

new(Args, Id) ->
    SchemaLocation = proplists:get_value(schema_location, Args),
    KeyGenSpec = proplists:get_value(keygenspec, Args),
    IntValueGen = basho_bench_keygen:new(KeyGenSpec, Id),

    case read_json_file(SchemaLocation) of
        {ok, JsonData} -> fun() -> generate_json_value(JsonData, IntValueGen) end;
        Error -> ?ERROR("Error creating basho_ench_valgen_json", []), Error
    end.
    
%% erl_internal
read_json_file(FileLocation) ->
    case file:read_file(FileLocation) of
        {ok, Data} -> {ok, mochijson2:decode(Data)};
        {error, Reason} -> {error, Reason}
    end.

generate_json_value(Schema, IntValueGen) ->
    mochijson2:encode(create_document(Schema, IntValueGen)).

create_document({struct, Fields}, IntValueGen) ->
    create_document(Fields, [], IntValueGen()).

create_document([], Doc, _IntValue) ->
    {struct, lists:reverse(Doc)};
create_document([{Name,Type}|Rest], Doc, IntValue) ->
    Value = case Type of
        <<"string">> -> 
            list_to_binary(lists:flatten(io_lib:format("~p", [IntValue])));
        <<"integer">> -> 
            IntValue;
        <<"number">> -> 
            float(IntValue)
    end,
    create_document(Rest, [{Name, Value}|Doc], IntValue).

-ifdef(TEST).
new_test() ->
    Filename = "/tmp/bb_schema.json",
    Json = <<"{\"name\": \"string\", \"age\": \"integer\", \"x_factor\": \"number\"}">>,
    file:write_file(Filename, Json),

    Args = [{schema_location, Filename}, {keygenspec, {sequential_int, ?MAX_VALUE}}],
    ValGen = new(Args, undefined),

    ActualJson = ValGen(),
    {struct, Actual} = mochijson2:decode(ActualJson),

    io:format("New Result: ~p~n", [Actual]),

    ?assertEqual(<<"0">>, proplists:get_value(<<"name">>, Actual)),
    ?assertEqual(0, proplists:get_value(<<"age">>, Actual)),
    ?assertEqual(0.0, proplists:get_value(<<"x_factor">>, Actual)).

create_document_test() ->
    Id = 1,
    Json = "{\"name\": \"string\", \"age\": \"integer\", \"x_factor\": \"number\"}",
    {struct, Actual} = create_document(mochijson2:decode(Json), basho_bench_keygen:new({sequential_int, ?MAX_VALUE}, Id)),

    ?assertEqual(<<"0">>, proplists:get_value(<<"name">>, Actual)),
    ?assertEqual(0, proplists:get_value(<<"age">>, Actual)),
    ?assertEqual(0.0, proplists:get_value(<<"x_factor">>, Actual)).
-endif.