-module(basho_bench_valgen_json).

-export([new/2, new_fixed_schema/2]).

-include("basho_bench.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MAX_VALUE, 999999).

new(Args, Id) ->
    FieldCount = proplists:get_value(field_count, Args),
    FieldType = proplists:get_value(field_type, Args),
    KeyGenSpec = proplists:get_value(keygenspec, Args),
    IntValueGen = basho_bench_keygen:new(KeyGenSpec, Id),

    Schema = basho_bench_riak_json_utils:generate_schema(FieldCount, FieldType),
    SchemaProps = create_schema_proplist(Schema, []),

    fun() -> generate_json_value(SchemaProps, IntValueGen) end.


new_fixed_schema(Args, Id) ->
    SchemaLocation = proplists:get_value(schema_location, Args),
    KeyGenSpec = proplists:get_value(keygenspec, Args),
    IntValueGen = basho_bench_keygen:new(KeyGenSpec, Id),

    case read_json_file(SchemaLocation) of
        {ok, JsonData} -> 
            SchemaProps = create_schema_proplist(JsonData, []),
            fun() -> generate_json_value(SchemaProps, IntValueGen) end;
        Error -> ?ERROR("Error creating basho_ench_valgen_json", []), Error
    end.
    
%% erl_internal
read_json_file(FileLocation) ->
    case file:read_file(FileLocation) of
        {ok, Data} -> {ok, mochijson2:decode(Data)};
        {error, Reason} -> {error, Reason}
    end.

create_schema_proplist([], Props) ->
    lists:reverse(Props);
create_schema_proplist([FieldSpec|Rest], Props) ->
    {struct, [{<<"Field">>, FieldName}, {<<"Type">>, FieldType}]} = FieldSpec,
    create_schema_proplist(Rest, [{FieldName, FieldType} | Props]).

generate_json_value(Schema, IntValueGen) ->
    mochijson2:encode(create_document(Schema, IntValueGen)).

create_document(Fields, IntValueGen) ->
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
    Args = [{field_count, 10}, {field_type, "integer"},
        {keygenspec, {sequential_int, ?MAX_VALUE}}],
    ValGen = new(Args, undefined),

    ActualJson = ValGen(),
    {struct, Actual} = mochijson2:decode(ActualJson),

    io:format("~p", [Actual]),

    ?assertEqual(0, proplists:get_value(<<"1">>, Actual)),
    ?assertEqual(0, proplists:get_value(<<"10">>, Actual)).

new_fixed_schema_test() ->
    Filename = "/tmp/bb_schema.json",
    Json = <<"[{\"Field\": \"name\", \"Type\": \"string\"}, {\"Field\": \"age\", \"Type\": \"integer\"}, {\"Field\": \"x_factor\", \"Type\": \"number\"}]">>,
    file:write_file(Filename, Json),

    Args = [{schema_location, Filename}, {keygenspec, {sequential_int, ?MAX_VALUE}}],
    ValGen = new_fixed_schema(Args, undefined),

    ActualJson = ValGen(),
    {struct, Actual} = mochijson2:decode(ActualJson),

    io:format("New Result: ~p~n", [Actual]),

    ?assertEqual(<<"0">>, proplists:get_value(<<"name">>, Actual)),
    ?assertEqual(0, proplists:get_value(<<"age">>, Actual)),
    ?assertEqual(0.0, proplists:get_value(<<"x_factor">>, Actual)).

create_document_test() ->
    Id = 1,
    Schema = [{<<"name">>, <<"string">>}, {<<"age">>, <<"integer">>}, {<<"x_factor">>, <<"number">>}],
    {struct, Actual} = create_document(Schema, basho_bench_keygen:new({sequential_int, ?MAX_VALUE}, Id)),

    ?assertEqual(<<"0">>, proplists:get_value(<<"name">>, Actual)),
    ?assertEqual(0, proplists:get_value(<<"age">>, Actual)),
    ?assertEqual(0.0, proplists:get_value(<<"x_factor">>, Actual)).
-endif.