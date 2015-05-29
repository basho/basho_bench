%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2015 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% @doc This is a driver for lager inside of the basho_bench framework.
%% Since lager is itself a dependency of basho_bench, you'll need to use
%% the `{d, no_lager}' macro in the `{erl_opt}' portion of rebar.config 
%% to build a basho_bench which does NOT use lager for its own log messages.
%%
%% Instead basho_bench will use the built-in SASL error_logger, so internal log
%% messages will show up in the `sasl.log.txt' file for a given test run.
%%
-module(basho_bench_driver_lager).

-export([new/1,
         run/4]).

-export([keygen/1, valgen/1, sink_generator/0, lager_msg_generator/0]).

-include("basho_bench.hrl").
-include_lib("lager/include/lager.hrl").

-record(state, {
          multiple_sink_support = false,
          current_backends = []
    }).

-define(TRACE_FILTER, [{trace, <<"match">>}]).

%% ====================================================================
%% API
%% ====================================================================

do_init() ->
    %% I guess it's mildly evil to use the process dictionary to store
    %% stateful things...

    Sinks = basho_bench_config:get(lager_sinks, []),
    erlang:put(lager_sinks, Sinks),

    Levels = basho_bench_config:get(lager_levels, []),
    case Levels == [] of
        true -> erlang:put(lager_levels, ?LEVELS);
        false -> erlang:put(lager_levels, Levels)
    end,

    MDKeys = basho_bench_config:get(lager_metadata_keys, [bar, baz, qux, hoge]),
    erlang:put(lager_mdkeys, MDKeys),

    erlang:put(lager_extra_sinks, basho_bench_config:get(extra_sinks, [])),

    erlang:function_exported(lager, log, 5).

new(1) ->
    MultSinks = do_init(),

    %% ok, at this point we need to start lager
    application:load(lager),
    %% do not hijack error_logger
    application:set_env(lager, error_logger_redirect, false),
    %% set the output handlers
    application:set_env(lager,
                        handlers,
                        [{lager_console_backend, debug},
                         {lager_file_backend, [{file, "console.log"}, {level, debug}, {size, 10485760}, {date, "$D0"}, {count, 5}]}
                        ]),
    application:set_env(lager, crash_log, "crash.log"),
    application:set_env(lager, extra_sinks, erlang:get(lager_extra_sinks)),

    lager:start(),

    configure_traces(basho_bench_config:get(traces, [])),

    {ok, #state{multiple_sink_support = MultSinks,
                current_backends = collect_backends(MultSinks)}};

new(_ID) -> 
    MultSinks = do_init(),
    {ok, #state{multiple_sink_support = MultSinks,
                current_backends = collect_backends(MultSinks)}}.

%% New lager
collect_backends(true) ->
    Handlers = lager_config:global_get(handlers, []),
    lists:foldl(fun({Id, _Pid, Sink}, Accum) ->
                        orddict:append(Sink, Id, Accum)
                end,
                orddict:new(),
                Handlers);
%% Old lager
collect_backends(false) ->
    Handlers = gen_event:which_handlers(lager_event),
    orddict:store(lager_event, Handlers, orddict:new()).

configure_trace(file) ->
    lager:trace_file("trace-error.log", ?TRACE_FILTER);
configure_trace(console) ->
    lager:trace_console(?TRACE_FILTER).

configure_traces(Traces) ->
    lists:foreach(fun configure_trace/1, Traces).

run_aux(SinkGen, ValueGen, State = #state{multiple_sink_support = S}, ExtraMD) ->
    Sink = SinkGen(),
    {Level, Metadata, Format, Args} = ValueGen(),
    Result = case S of
        true ->
            lager:log(Sink, Level, Metadata ++ ExtraMD, Format, Args);
        false ->
            lager:log(Level, Metadata ++ ExtraMD, Format, Args)
    end,
    case Result of
        ok -> {ok, State};
        {error, lager_not_running} -> {'EXIT', lager_not_running};
        {error, Reason} -> {error, Reason, State}
    end.

change_loglevel(Sink, Backends, true) ->
    lists:foreach(fun(B) -> L = random_loglevel(),
                            error_logger:info_msg("Changing loglevel on ~p (~p)~n",
                                      [B, L]),
                            lager:set_loglevel(Sink, B, undefined, L) end,
                  Backends);
change_loglevel(_Sink, Backends, false) ->
    lists:foreach(fun(B) -> L = random_loglevel(),
                            error_logger:info_msg("Changing loglevel on ~p (~p)~n",
                                      [B, L]),
                            lager:set_loglevel(B, L) end,
                  Backends).


random_loglevel() ->
    get_random(erlang:get(lager_levels), debug).

run(change_loglevel, _SinkGen, _ValueGen, State=#state{current_backends=B,multiple_sink_support=M}) ->
    lists:foreach(fun(Sink) -> change_loglevel(Sink,
                                               orddict:fetch(Sink, B), M) end,
                  orddict:fetch_keys(B)),
    {ok, State};
run(log, SinkGen, ValueGen, State) ->
    run_aux(SinkGen, ValueGen, State, []);
run(trace, SinkGen, ValueGen, State) ->
    run_aux(SinkGen, ValueGen, State, ?TRACE_FILTER).

keygen(_Id) ->
    fun sink_generator/0.

valgen(_Id) ->
    fun lager_msg_generator/0.

sink_generator() ->
    Sinks = erlang:get(lager_sinks),
    get_random(Sinks, lager_event). % TODO - this is hard coded for now because DEFAULT_SINK macro is only in the multiple-sink branch

lager_msg_generator() ->
    Level = get_random(erlang:get(lager_levels), debug),
    Metadata = maybe_generate_metadata(),
    Args = maybe_generate_args(),
    Fmt = generate_fmt(Args),
    {Level, Metadata, Fmt, Args}.

get_random(List) ->
    get_random(List, undefined).

get_random(List, Default) ->
    Len = length(List),
    case Len of
        0 -> Default;
        1 -> hd(List);
        _ -> lists:nth(random:uniform(Len), List)
    end.

maybe_generate_metadata() ->
    NumArgs = random:uniform(3) - 1,
    generate_md(NumArgs, []).

generate_md(0, Acc) -> lists:reverse(Acc);
generate_md(N, Acc) ->
    MDKeys = erlang:get(lager_mdkeys),
    Data = case random:uniform(100) of
               X when X rem 10 =:= 0 ->
                   random_binstr();
               X -> X
    end,
    generate_md(N - 1, [ { get_random(MDKeys), Data } | Acc ]).

maybe_generate_args() ->
    NumArgs = random:uniform(6) - 1,
    generate_args(NumArgs, []).

generate_args(0, Acc) -> lists:reverse(Acc);
generate_args(N, Acc) ->
    generate_args(N - 1, [ random_binstr() | Acc ]).

generate_fmt(Args) ->
    L = length(Args),
    case L of
        0 -> "No arguments!";
        _ -> string:copies("~p ", L)
    end.

random_binstr() ->
    Char = random:uniform(26) + 64, % 64 precedes ASCII "A" (65), so this will generate a char in the range of A-Z
    Num  = random:uniform(50),
    list_to_binary(string:chars(Char, Num)).
