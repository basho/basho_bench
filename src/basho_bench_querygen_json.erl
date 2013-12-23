-module(basho_bench_querygen_json).

-export([new_index_query/2, new_range_query/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MAX_VALUE, 99999).
-define(RANGE_LENGTH, 100).

new_index_query(Args, Id) ->
	KeySpec = proplists:get_value(keygenspec, Args),
	KeyGen = basho_bench_keygen:new(KeySpec, Id),
	fun() -> get_index_query(Id, [{value, KeyGen()}|Args]) end.

new_range_query(Args, Id) ->
	KeySpec = proplists:get_value(keygenspec, Args),
	RangeLength = proplists:get_value(range_length, Args, ?RANGE_LENGTH),
	KeyGen = basho_bench_keygen:new(KeySpec, Id),
	RangeFun = fun() -> Value = KeyGen(), [{start, Value}, {stop, Value + RangeLength}] end,
	fun() -> get_range_query(Id, RangeFun() ++ Args) end.


%% internal

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
	iolist_to_binary(mochijson2:encode(ToEncode)).

encode_value(Value) ->
	if
		is_integer(Value) ->
			Value;
		is_float(Value) ->
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
	{struct, [{list_to_binary(Conjunction), Terms}]}.

eq(Field, Value) ->
	{struct, [{list_to_binary(Field), encode_value(Value)}]}.

op(Operator, Field, Value) ->
	{struct, [{list_to_binary(Field), {struct, [{list_to_binary(Operator), encode_value(Value)}]}}]}.



-ifdef(TEST).
	
	new_index_query_test() ->
		Args = [{field, "Field"}, {keygenspec, {sequential_int, ?MAX_VALUE}}],
		Expected = <<"{\"Field\":0}">>,
		Generator = new_index_query(Args, undefined),
		Actual = Generator(),

		?assertEqual(Expected, Actual).

	new_range_query_test() ->
		Args = [{field, "Field"}, {range_length, ?RANGE_LENGTH}, {keygenspec, {sequential_int, ?MAX_VALUE}}],
		StopValue = 0 + ?RANGE_LENGTH,
		Expected = list_to_binary(io_lib:format("{\"Field\":{\"$between\":[0,~p]}}", [StopValue])),
		Generator = new_range_query(Args, undefined),
		Actual = Generator(),

		?assertEqual(Expected, Actual).
		
	get_index_query_test() ->
		Expected = <<"{\"Field\":\"Value\"}">>,
		Actual = get_index_query(undefined, [{collection, "test_collection"}, {field, "Field"}, {value, "Value"}]),

		?assertEqual(Expected, Actual).

	get_range_query_test() ->
		Expected = <<"{\"Field\":{\"$between\":[1,10]}}">>,
		Actual = get_range_query(undefined, [{collection, "test_collection"}, {field, "Field"}, {start, 1}, {stop, 10}]),

		?assertEqual(Expected, Actual).

	conjunct_test() ->
		Expected = {struct, [{<<"$and">>, [{struct, [{<<"Field1">>, <<"Value1">>}]},
										   {struct, [{<<"Field2">>, <<"Value2">>}]}]}]},
		Actual = conjunct("$and", [eq("Field1", "Value1"), eq("Field2", "Value2")]),

		?assertEqual(Expected, Actual).

	eq_test() ->
		Expected = {struct, [{<<"Field">>, <<"Value">>}]},
		Actual = eq("Field", "Value"),

		?assertEqual(Expected, Actual).
-endif.