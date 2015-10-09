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

-module(basho_bench_stats_writer_riemann).

-export([new/2,
         terminate/1,
         process_summary/5,
         report_error/3,
         report_latency/7]).

-include("basho_bench.hrl").

new(_, _) ->
    ?INFO("module=~s event=start stats_sink=riemann\n", [?MODULE]),
    katja:start().

terminate(_) ->
    ?INFO("module=~s event=stop stats_sink=riemann\n", [?MODULE]),
    katja:stop(),
    ok.

process_summary(_, _Elapsed, _Window, Oks, Errors) ->
    katja:send_entities([{events, [[{service, "basho_bench summary ok"},
                                    {metric, Oks}],
                                   [{service, "basho_bench summary errors"},
                                    {metric, Errors}]]}]).

report_error(_, Key, Count) ->
   katja:send_event([{service, io_lib:format("basho_bench error for key ~p", [Key])},
                     {metric, Count}]).

report_latency(_, _Elapsed, _Window, Op,
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
