%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2014 Basho Techonologies
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
%% HOWTO:
%%
%% * To run basho_bench with the default CSV writer, nothing needs to
%%   be done. But if wanting to override a former setting, then
%%   writing the following in the benchmark config file will switch
%%   the stats writer to CSV:
%%
%%    {stats, {csv}}.
%%
%% * To run basho_bench with statistics sent to [Riemann][1], in the
%%   benchmark config file the following needs to be written:
%%
%%    {stats, {riemann}}.
%%
%%   This will, by default, try to connect to a Riemann server on
%%   localhost, port 5555, and will not set any TTL or tags. To
%%   configure the writer, an app config needs to be written. For
%%   that, one needs to add "-config app.config" (the filename can be
%%   anything) to escript_emu_args in rebar.config, recompile
%%   basho_bench, and add the necessary configuration to app.config,
%%   something along these lines:
%%
%%    [
%%      {katja, [
%%        {host, "127.0.0.1"},
%%        {port, 5555},
%%        {transport, detect},
%%        {pool, []},
%%        {defaults, [{host, "myhost.local"},
%%                    {tags, ["basho_bench"]},
%%                    {ttl, 5.0}]}
%%      ]}
%%    ].

-module(basho_bench_stats_writer).

-export([new/3,
         terminate/1,
         process_summary/5,
         report_error/3,
         report_latency/7]).

-include("basho_bench.hrl").

new({csv}, Ops, Measurements) ->
    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, X}, op_csv_file(X)) || X <- Ops],

    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, X}, measurement_csv_file(X)) || X <- Measurements],

    TestDir = basho_bench:get_test_dir(),
    %% Setup output file w/ counters for total requests, errors, etc.
    {ok, SummaryFile} = file:open(
        filename:join([TestDir, "/summary.csv"]),
        [raw, binary, write]
    ),
    file:write(SummaryFile, <<"elapsed, window, total, successful, failed\n">>),

    %% Setup errors file w/counters for each error.  Embedded commas likely
    %% in the error messages so quote the columns.
    {ok, ErrorsFile} = file:open(
        filename:join([TestDir, "errors.csv"]),
        [raw, binary, write]
    ),
    file:write(ErrorsFile, <<"\"error\",\"count\"\n">>),

    {SummaryFile, ErrorsFile};
new({riemann}, _, _) ->
    katja:start().

terminate({{csv}, {SummaryFile, ErrorsFile}}) ->
    [ok = file:close(F) || {{csv_file, _}, F} <- erlang:get()],
    ok = file:close(SummaryFile),
    ok = file:close(ErrorsFile),
    ok;
terminate({{riemann}, _}) ->
    katja:stop(),
    ok.

process_summary({{csv}, {SummaryFile, _ErrorsFile}},
                Elapsed, Window, Oks, Errors) ->
    file:write(SummaryFile,
               io_lib:format("~w, ~w, ~w, ~w, ~w\n",
                             [Elapsed,
                              Window,
                              Oks + Errors,
                              Oks,
                              Errors]));
process_summary({{riemann}, _},
                _Elapsed, _Window, Oks, Errors) ->
    katja:send_entities([{events, [[{service, "basho_bench summary ok"},
                                    {metric, Oks}],
                                   [{service, "basho_bench summary errors"},
                                    {metric, Errors}]]}]).

report_error({{csv}, {_SummaryFile, ErrorsFile}},
             Key, Count) ->
    file:write(ErrorsFile,
               io_lib:format("\"~w\",\"~w\"\n",
                             [Key, Count]));
report_error({{riemann}, _},
            Key, Count) ->
   katja:send_event([{service, io_lib:format("basho_bench error for key ~p", [Key])},
                     {metric, Count}]).

report_latency({{csv}, {_SummaryFile, _ErrorsFile}},
               Elapsed, Window, Op,
               Stats, Errors, Units) ->
    case proplists:get_value(n, Stats) > 0 of
        true ->
            P = proplists:get_value(percentile, Stats),
            Line = io_lib:format("~w, ~w, ~w, ~w, ~.1f, ~w, ~w, ~w, ~w, ~w, ~w\n",
                                 [Elapsed,
                                  Window,
                                  Units,
                                  proplists:get_value(min, Stats),
                                  proplists:get_value(arithmetic_mean, Stats),
                                  proplists:get_value(median, Stats),
                                  proplists:get_value(95, P),
                                  proplists:get_value(99, P),
                                  proplists:get_value(999, P),
                                  proplists:get_value(max, Stats),
                                  Errors]);
        false ->
            ?WARN("No data for op: ~p\n", [Op]),
            Line = io_lib:format("~w, ~w, 0, 0, 0, 0, 0, 0, 0, 0, ~w\n",
                                 [Elapsed,
                                  Window,
                                  Errors])
    end,
    file:write(erlang:get({csv_file, Op}), Line);
report_latency({{riemann}, _},
               _Elapsed, _Window, Op,
               Stats, Errors, Units) ->
    case proplists:get_value(n, Stats) > 0 of
        true ->
            katja:send_entities([{events, riemann_op_latencies(Op, Stats, Errors, Units)}]);
        false ->
            ?WARN("No data for op: ~p\n", [Op])
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

op_csv_file({Label, _Op}) ->
    TestDir = basho_bench:get_test_dir(),
    Fname = filename:join([TestDir, normalize_label(Label) ++ "_latencies.csv"]),
    {ok, F} = file:open(Fname, [raw, binary, write]),
    ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
    F.

measurement_csv_file({Label, _Op}) ->
    TestDir = basho_bench:get_test_dir(),
    Fname = filename:join([TestDir, normalize_label(Label) ++ "_measurements.csv"]),
    {ok, F} = file:open(Fname, [raw, binary, write]),
    ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
    F.

normalize_label(Label) when is_list(Label) ->
    replace_special_chars(Label);
normalize_label(Label) when is_binary(Label) ->
    normalize_label(binary_to_list(Label));
normalize_label(Label) when is_integer(Label) ->
    normalize_label(integer_to_list(Label));
normalize_label(Label) when is_atom(Label) ->
    normalize_label(atom_to_list(Label));
normalize_label(Label) when is_tuple(Label) ->
    Parts = [normalize_label(X) || X <- tuple_to_list(Label)],
    string:join(Parts, "-").

replace_special_chars([H|T]) when
      (H >= $0 andalso H =< $9) orelse
      (H >= $A andalso H =< $Z) orelse
      (H >= $a andalso H =< $z) ->
    [H|replace_special_chars(T)];
replace_special_chars([_|T]) ->
    [$-|replace_special_chars(T)];
replace_special_chars([]) ->
    [].

riemann_op_latencies({Label, _Op}, Stats, Errors, Units) ->
    P = proplists:get_value(percentile, Stats),
    Service = normalize_label(Label),

    [[{service, io_lib:format("basho_bench op ~s latency min", [Service])},
      {metric, proplists:get_value(min, Stats)}],
     [{service, io_lib:format("basho_bench op ~s latency max", [Service])},
      {metric, proplists:get_value(max, Stats)}],
     [{service, io_lib:format("basho_bench op ~s latency mean", [Service])},
      {metric, proplists:get_value(arithmetic_mean, Stats)}],
     [{service, io_lib:format("basho_bench op ~s latency median", [Service])},
      {metric, proplists:get_value(median, Stats)}],
     [{service, io_lib:format("basho_bench op ~s latency 95%", [Service])},
      {metric, proplists:get_value(95, P)}],
     [{service, io_lib:format("basho_bench op ~s latency 99%", [Service])},
      {metric, proplists:get_value(99, P)}],
     [{service, io_lib:format("basho_bench op ~s latency 99.9%", [Service])},
      {metric, proplists:get_value(999, P)}],
     [{service, io_lib:format("basho_bench op ~s #", [Service])},
      {metric, Units}],
     [{service, io_lib:format("basho_bench op ~s error#", [Service])},
      {metric, Errors}]].
