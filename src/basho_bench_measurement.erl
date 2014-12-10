%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
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
-module(basho_bench_measurement).

-behaviour(gen_server).

%% API
-export([start_link/0,
         run/0,
         take_measurement/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { driver,
                 driver_state,
                 timer_refs = []
               }).

-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

run() ->
    gen_server:cast(?MODULE, run).

take_measurement(Measurement) ->
    gen_server:call(?MODULE, {take_measurement, Measurement}, infinity).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Pull all config settings from environment
    Driver = basho_bench_config:get(measurement_driver),
    case catch(Driver:new()) of
        {ok, DriverState} ->
            State = #state {
              driver = Driver,
              driver_state = DriverState },
            {ok, State};
        Error ->
            ?FAIL_MSG("Failed to initialize driver ~p: ~p\n", [Driver, Error]),
            {stop, Error}
    end.

handle_call(run, _From, State) ->
    NewState = restart_measurements(State),
    {reply, ok, NewState};
handle_call({take_measurement, Measurement}, _From, State) ->
    Driver = State#state.driver,
    DriverState = State#state.driver_state,
    {_Label, MeasurementTag} = Measurement,
    Result = (catch Driver:run(MeasurementTag, DriverState)),
    case Result of
        {ok, Value, NewDriverState} ->
            basho_bench_stats:op_complete(Measurement, ok, Value),
            {reply, ok, State#state { driver_state = NewDriverState}};

        {error, Reason, NewDriverState} ->
            %% Driver encountered a recoverable error
            basho_bench_stats:op_complete(Measurement, {error, Reason}, 0),
            {reply, ok, State#state { driver_state = NewDriverState}};

        {'EXIT', Reason} ->
            %% Driver crashed, generate a crash error and terminate. This will take down
            %% the corresponding measurement which will get restarted by the appropriate supervisor.
            basho_bench_stats:op_complete(Measurement, {error, crash}, 0),

            %% Give the driver a chance to cleanup
            (catch Driver:terminate({'EXIT', Reason}, DriverState)),

            ?DEBUG("Driver ~p crashed: ~p\n", [Driver, Reason]),
            {stop, crash, State};

        {stop, Reason} ->
            %% Driver (or something within it) has requested that this measurement
            %% terminate cleanly.
            ?INFO("Driver ~p (~p) has requested stop: ~p\n", [Driver, self(), Reason]),

            %% Give the driver a chance to cleanup
            (catch Driver:terminate(normal, DriverState)),

            {stop, normal, State}
    end.


handle_cast(run, State) ->
    NewState = restart_measurements(State),
    {noreply, NewState}.

handle_info(Msg, State) ->
    io:format("[~s:~p] DEBUG - Msg: ~p~n", [?MODULE, ?LINE, Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

restart_measurements(State) ->
    [timer:cancel(X) || X <- State#state.timer_refs],
    F =
        fun({MeasurementTag, IntervalMS}) ->
                {ok, TRef} = timer:apply_interval(IntervalMS, ?MODULE, take_measurement, [{MeasurementTag, MeasurementTag}]),
                TRef;
           ({Label, MeasurementTag, IntervalMS}) ->
                {ok, TRef} = timer:apply_interval(IntervalMS, ?MODULE, take_measurement, [{Label, MeasurementTag}]),
                TRef
        end,
    TRefs = [F(X) || X <- basho_bench_config:get(measurements, [])],
    State#state { timer_refs = TRefs }.
