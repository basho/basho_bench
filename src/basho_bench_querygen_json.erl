-module(basho_bench_querygen_json).

-export([new_index_query/2, new_random_index_query/2, 
	     new_range_query/2, new_random_range_query/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MAX_VALUE, 99999).
-define(RANGE_LENGTH, 100).

new_index_query(Args, Id) ->
	KeySpec = proplists:get_value(keygenspec, Args),
	KeyGen = basho_bench_keygen:new(KeySpec, Id),
	fun() -> get_index_query(Id, [{value, KeyGen()}|Args]) end.

new_random_index_query(Args, Id) ->
	FieldCount = proplists:get_value(field_count, Args),
    FieldType = proplists:get_value(field_type, Args),
    Schema = basho_bench_riak_json_utils:generate_schema(FieldCount, FieldType),
    FieldGen = fun() -> basho_bench_riak_json_utils:random_field_name(Schema) end,
	KeySpec = proplists:get_value(keygenspec, Args),
	KeyGen = basho_bench_keygen:new(KeySpec, Id),
	fun() -> get_index_query(Id, [{value, KeyGen()},{field, FieldGen()}|Args]) end.

new_range_query(Args, Id) ->
	KeySpec = proplists:get_value(keygenspec, Args),
	RangeLength = proplists:get_value(range_length, Args, ?RANGE_LENGTH),
	KeyGen = basho_bench_keygen:new(KeySpec, Id),
	RangeFun = fun() -> Value = KeyGen(), [{start, Value}, {stop, Value + RangeLength}] end,
	fun() -> get_range_query(Id, RangeFun() ++ Args) end.

new_random_range_query(Args, Id) ->
	FieldCount = proplists:get_value(field_count, Args),
    FieldType = proplists:get_value(field_type, Args),
    Schema = basho_bench_riak_json_utils:generate_schema(FieldCount, FieldType),

	RangeLength = proplists:get_value(range_length, Args, ?RANGE_LENGTH),

	KeySpec = proplists:get_value(keygenspec, Args),
	KeyGen = basho_bench_keygen:new(KeySpec, Id),
	
	FieldFun = fun() -> basho_bench_riak_json_utils:random_field_name(Schema) end,
	RangeFun = fun() -> Value = KeyGen(), [{start, Value}, {stop, Value + RangeLength}] end,
	fun() -> get_range_query(Id, RangeFun() ++ [{field, FieldFun()}|Args]) end.


%% internal
% get_range(NumGen, RangeLength) ->
% 	Value = NumGen(),
% 	if
% 	 	is_number(Value) -> [{start, Value}, {stop, Value + RangeLength}];
% 	 	true -> lager:debug("Generated invalid number: ~p", [Value]),
% 	 		[{start, 0}, {stop, RangeLength}]
% 	end.
	 

read_json_file(FileLocation) ->
    case file:read_file(FileLocation) of
        {ok, Data} -> {ok, mochijson2:decode(Data)};
        {error, Reason} -> {error, Reason}
    end.

get_index_query(_Id, Args) ->
	Field = proplists:get_value(field, Args),
	Value = proplists:get_value(value, Args),
	DataStruct = eq(Field, Value),
	encode(DataStruct).

get_range_query(_Id, Args) ->
	Field = proplists:get_value(field, Args),
	Start = proplists:get_value(start, Args),
	Stop = proplists:get_value(stop, Args),
	DataStruct = op("$between", Field, [Start, Stop]),
	encode(DataStruct).

encode(ToEncode) ->
	case catch iolist_to_binary(mochijson2:encode(ToEncode)) of
		{'EXIT', {Exception,Reason}} ->
			lager:debug("Couldn't encode ~p ~p:~p", [ToEncode, Exception, Reason]);
		Result -> Result
	end.

encode_value(Value) ->
	if
		is_integer(Value) ->
			Value;
		is_float(Value) ->
			Value;
		is_binary(Value) ->
			Value;
		true ->
			case io_lib:printable_list(Value) of
				true -> list_to_binary(Value);
				false -> encode_list(Value, [])
			end
	end.

encode_list([], List) ->
	lists:reverse(List);
encode_list([Item|Rest], List) ->
	encode_list(Rest, [encode_value(Item)|List]).

conjunct(Conjunction, Terms) ->
	{struct, [{Conjunction, Terms}]}.

eq(Field, Value) ->
	{struct, [{Field, encode_value(Value)}]}.

op(Operator, Field, [Start, Stop]) ->
	{struct, [{Field, {struct, [{list_to_binary(Operator), [encode_value(Start), encode_value(Stop)]}]}}]};
op(Operator, Field, Value) ->
	{struct, [{Field, {struct, [{list_to_binary(Operator), encode_value(Value)}]}}]}.



-ifdef(TEST).
	
	new_index_query_test() ->
		Args = [{field, <<"Field">>}, {keygenspec, {sequential_int, ?MAX_VALUE}}],
		Expected = <<"{\"Field\":0}">>,
		Generator = new_index_query(Args, undefined),
		Actual = Generator(),

		?assertEqual(Expected, Actual).

	new_random_index_query_test() ->
		Args = [{field_count, 1}, 
				{field_type, "integer"}, 
				{keygenspec, {sequential_int, ?MAX_VALUE}}],
		Expected = <<"{\"1\":0}">>,
		Generator = new_random_index_query(Args, undefined),
		Actual = Generator(),

		?assertEqual(Expected, Actual).

	new_range_query_test() ->
		Args = [{field, "Field"}, {range_length, ?RANGE_LENGTH}, {keygenspec, {sequential_int, ?MAX_VALUE}}],
		StopValue = 0 + ?RANGE_LENGTH,
		Expected = list_to_binary(io_lib:format("{\"Field\":{\"$between\":[0,~p]}}", [StopValue])),
		Generator = new_range_query(Args, undefined),
		Actual = Generator(),

		?assertEqual(Expected, Actual).

	new_random_range_query_test() ->
		Args = [{field_count, 1}, {field_type, "integer"},
				{keygenspec, {sequential_int, ?MAX_VALUE}}],
		StopValue = 0 + ?RANGE_LENGTH,
		Expected = list_to_binary(io_lib:format("{\"1\":{\"$between\":[0,~p]}}", [StopValue])),
		Generator = new_random_range_query(Args, undefined),
		Actual = Generator(),

		?assertEqual(Expected, Actual).
		
	get_index_query_test() ->
		Expected = <<"{\"Field\":\"Value\"}">>,
		Actual = get_index_query(undefined, [{collection, "test_collection"}, 
											 {field, "Field"}, 
											 {value, "Value"}]),

		?assertEqual(Expected, Actual).

	get_range_query_test() ->
		Expected = <<"{\"Field\":{\"$between\":[1,10]}}">>,
		Actual = get_range_query(undefined, [{collection, "test_collection"}, {field, "Field"}, {start, 1}, {stop, 10}]),

		?assertEqual(Expected, Actual).

	conjunct_test() ->
		Expected = {struct, [{<<"$and">>, [{struct, [{<<"Field1">>, <<"Value1">>}]},
										   {struct, [{<<"Field2">>, <<"Value2">>}]}]}]},
		Actual = conjunct(<<"$and">>, [eq(<<"Field1">>, <<"Value1">>), eq(<<"Field2">>, <<"Value2">>)]),

		?assertEqual(Expected, Actual).

	eq_test() ->
		Expected = {struct, [{<<"Field">>, <<"Value">>}]},
		Actual = eq(<<"Field">>, <<"Value">>),

		?assertEqual(Expected, Actual).
-endif.