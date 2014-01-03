-module(basho_bench_riak_json_utils).

-export([generate_schema/2, read_json_file/1, random_field_name/1]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

generate_schema(TermCount, FieldType) ->
	generate_schema(TermCount, FieldType, 0, []).

generate_schema(TermCount, _FieldType, FieldCount, Acc) when FieldCount >= TermCount ->
	lists:reverse(Acc);
generate_schema(TermCount, FieldType, FieldCount, Acc) ->
	[FieldName] = io_lib:format("~p", [FieldCount + 1]),
	Term = {struct, 
		[{<<"Field">>, list_to_binary(FieldName)}, {<<"Type">>, list_to_binary(FieldType)}]},
	generate_schema(TermCount, FieldType, FieldCount + 1, [Term|Acc]).

read_json_file(FileLocation) ->
    case file:read_file(FileLocation) of
        {ok, Data} -> {ok, mochijson2:decode(Data)};
        {error, Reason} -> {error, Reason}
    end.

random_field_name(Schema) ->
	Index = random:uniform(length(Schema)),
	{struct, [{<<"Field">>, FieldName}, {_,_}]} = lists:nth(Index, Schema),
	FieldName.


-ifdef(TEST).

generate_schema_test() ->
	Expected = [{struct, [{<<"Field">>, <<"1">>}, {<<"Type">>, <<"integer">>}]}],
	Actual = generate_schema(1, "integer"),

	?assertEqual(Expected, Actual).

random_field_name_test() ->
	Schema = generate_schema(10, "string"),

	Fun = fun(_Value, Acc) -> 
			RandomName = random_field_name(Schema),
			NameCount = proplists:get_value(RandomName, Acc, 0) + 1,
			[{RandomName,NameCount}|proplists:delete(RandomName, Acc)]
		  end,

	TermCounts = lists:foldl(Fun, [], Schema),

	?assert(length(TermCounts) > 1).

-endif.
