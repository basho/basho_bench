-module(basho_bench_stats_writer_ets).

-export([new/2,
         terminate/1,
         process_summary/5,
         report_error/3,
         report_latency/7]).

-include("basho_bench.hrl").


new(_Ops, _Measurements) ->
    ?INFO("module=~s event=start stats_sink=ets\n", [?MODULE]),
    ok = ets:new(basho_bench_results_summary, [set, named_table]),
    ok = ets:new(basho_bench_results_errors, [set, named_table]),
    ok.


terminate(ok) ->
    ?INFO("module=~s event=stop stats_sink=ets\n", [?MODULE]),
    ok.


process_summary(ok, Elapsed, Window, Oks, Errors) ->
    Record = {Elapsed, Window, Oks + Errors, Oks, Errors},
    ets:insert(basho_bench_results_summary, Record).


report_error(ok, Key, Count) ->
    ets:insert(basho_bench_results_errors, {Key, Count}).


report_latency(ok, Elapsed, Window, Op, Stats, Errors, Units) ->
    Record = case proplists:get_value(n, Stats) > 0 of
        true ->
            P = proplists:get_value(percentile, Stats),
            {
                Elapsed,
                Window,
                Units,
                proplists:get_value(min, Stats),
                proplists:get_value(arithmetic_mean, Stats),
                proplists:get_value(median, Stats),
                proplists:get_value(95, P),
                proplists:get_value(99, P),
                proplists:get_value(999, P),
                proplists:get_value(max, Stats),
                Errors
            };
        false ->
            ?WARN("No data for op: ~p\n", [Op]),
            {Op, Elapsed, Window, 0, 0, 0, 0, 0, 0, 0, 0, Errors}
    end,
    ets:insert(basho_bench_results_summary, Record).
