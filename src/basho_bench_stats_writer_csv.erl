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
%%.

-module(basho_bench_stats_writer_csv).

-export([new/2,
         terminate/1,
         process_summary/5,
         report_error/3,
         report_global_stats/4,
         report_latency/7]).

-include("basho_bench.hrl").

new(Ops, Measurements) ->
    ?INFO("module=~s event=start stats_sink=csv\n", [?MODULE]),
    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, X}, op_csv_file(X)) || X <- Ops],

    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, X}, measurement_csv_file(X)) || X <- Measurements],

    %% Initialize an empty set of json statistics
    erlang:put(run_metrics, [ {X,""} || {X,_} <- Ops]),

    TestDir = basho_bench:get_test_dir(),
    %% Setup output file w/ counters for total requests, errors, etc.
    {ok, SummaryFile} = file:open(
        filename:join([TestDir, "summary.csv"]),
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

    {SummaryFile, ErrorsFile}.

terminate({SummaryFile, ErrorsFile}) ->
    ?INFO("module=~s event=stop stats_sink=csv\n", [?MODULE]),

    %% write out run-wide statistics before exiting
    RunStatsType = basho_bench_config:get(run_statistics_output_format, csv),
    dump_run_statistics(RunStatsType),

    [ok = file:close(F) || {{csv_file, _}, F} <- erlang:get()],
    ok = file:close(SummaryFile),
    ok = file:close(ErrorsFile),
    ok.

process_summary({SummaryFile, _ErrorsFile},
                Elapsed, Window, Oks, Errors) ->
    file:write(SummaryFile,
               io_lib:format("~w, ~w, ~w, ~w, ~w\n",
                             [Elapsed,
                              Window,
                              Oks + Errors,
                              Oks,
                              Errors])).

report_error({_SummaryFile, ErrorsFile},
             Key, Count) ->
    file:write(ErrorsFile,
               io_lib:format("\"~w\",\"~w\"\n",
                             [Key, Count])).

report_global_stats({Op,_}, Stats, Errors, Units) ->
    %% Build up JSON structure representing statistices collected in folsom
    P = proplists:get_value(percentile, Stats),
    JsonElements0 = lists:foldl(fun(K, Acc) ->
        case K of
            %% Unpack percentiles list
            percentile ->
                [
                    % we skip p50 since we have the median
                    {p75, proplists:get_value(75, P)},
                    {p90, proplists:get_value(90, P)},
                    {p95, proplists:get_value(95, P)},
                    {p99, proplists:get_value(99, P)},
                    {p999, proplists:get_value(999, P)}
                ] ++ Acc;

            %% We ignore the histogram as it will not be used in the analysis
            histogram -> Acc;

            %% Rename arithmetic_mean to mean {<key>, <value>}
            arithmetic_mean ->
                [{mean, proplists:get_value(K, Stats)} | Acc];

            %% All other metrics get turned into {<key>, <value>}
            _ ->
                [{K, proplists:get_value(K, Stats)} | Acc]
        end
    end, [], proplists:get_keys(Stats)),

    %% insert correct units count
    JsonElements1 = lists:keyreplace(n, 1, JsonElements0, {'n', Units}),

    %% insert error counts
    JsonElements2 = [{basho_errors, Errors} | JsonElements1],

    JsonMetrics0 = erlang:get(run_metrics),
    JsonMetrics = lists:keyreplace(Op, 1, JsonMetrics0, {Op, {JsonElements2}}),
    %?DEBUG("Generated Json:\n~w",[JsonElements]),
    erlang:put(run_metrics, JsonMetrics).

report_latency({_SummaryFile, _ErrorsFile},
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
    file:write(erlang:get({csv_file, Op}), Line).

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



dump_run_statistics(RunStatsType) ->
    Lines = stringify_stats(RunStatsType, erlang:get(run_metrics)),
    TestDir = basho_bench:get_test_dir(),
    FileName = filename:join([TestDir, "run_statistics." ++ atom_to_list(RunStatsType)]),
    write_run_statistics(FileName, Lines).

stringify_stats(_RunStatsType=json, RunMetrics) ->
    [ jiffy:encode( {[{recordedMetrics, {RunMetrics}}]}, [pretty] ) ];
stringify_stats(_RunStatsType=csv, RunMetrics) ->
    % Ordered output of fields
    OrderedFields = [n, mean, geometric_mean, harmonic_mean,
        variance, standard_deviation, skewness, kurtosis,
        median, p75, p90, p95, p99, p999,
        min, max, basho_errors],

    %% Remove all duplicates and sort list of available headers
    StrHeader = string:join(
        lists:map(fun atom_to_list/1, [op|OrderedFields]), ", "),
    StringyfiedHeader =  io_lib:format("~s\n", [StrHeader]),

    %% Loop over each metric and build each line of the CSV file and write it out
    Lines = lists:foldl(fun(Op, Lines) ->
        {MetricsObj} = proplists:get_value(Op, RunMetrics),
        CsvLine = lists:foldl(fun(K, Acc) ->
            StrVal = io_lib:format("~w", [proplists:get_value(K, MetricsObj, "")]),
            [StrVal|Acc]
        end, [], OrderedFields),

        OpStr = string:join( [atom_to_list(Op) | lists:reverse(CsvLine)], ", "),
        [io_lib:format("~s\n", [OpStr]) | Lines]

    end, [], proplists:get_keys(RunMetrics)),
    [StringyfiedHeader|Lines].

write_run_statistics(FileName, Lines)->
    {ok, GlobalMetricsFile} = file:open(
        FileName,
        [raw, binary, write]
    ),
    
    %% Write lines to file
    lists:foreach(fun(Line) ->
        ok = file:write(GlobalMetricsFile, Line)
    end, Lines),
    ok = file:close(GlobalMetricsFile).

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

