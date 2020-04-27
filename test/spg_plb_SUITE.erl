%%-------------------------------------------------------------------
%% @author Maxim Fedorov <maximfca@gmail.com>
%%     Process Groups smoke test.
-module(spg_plb_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    groups/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases exports
-export([
    lb/0, lb/1,
    lb_cpu/0, lb_cpu/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2
]).

-behaviour(gen_server).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 30}}].


init_per_testcase(TestCase, Config) ->
    {ok, Pid} = spg_plb:start_link(TestCase),
    [{plb, Pid} | Config].

end_per_testcase(_TestCase, Config) ->
    gen_server:stop(?config(plb, Config)),
    ok.

all() ->
    [{group, parallel}, {group, sequence}].

groups() ->
    [
        {parallel, [parallel], [lb]},
        {sequence, [sequence], [lb_cpu]}
    ].

%%--------------------------------------------------------------------
%% gen_server implementation

-record(state, {
    count = 0 :: non_neg_integer(),
    store = #{} :: #{atom() => [integer()]},
    delay = 0 :: non_neg_integer()}).

start(Delay) ->
    {ok, Pid} = gen_server:start_link(?MODULE, Delay, []),
    Pid.

init(Delay) when is_integer(Delay) ->
    {ok, #state{delay = Delay}}.

handle_call({get, Key}, _From, #state{store = Store, count = Count} = State) ->
    {reply, maps:get(Key, Store, []), State#state{count = Count + 1}};

handle_call(get_count, _From, State) ->
    {reply, State#state.count, State}.

handle_cast({put, Key, Value}, #state{store = Store, count = Count, delay = Delay} = State) ->
    Delay > 0 andalso timer:sleep(Delay),
    {noreply, State#state{count = Count + 1, store = Store#{Key => [Value | maps:get(Key, Store, [])]}}};

handle_cast(count, #state{count = Count, delay = Delay} = State) ->
    Delay > 0 andalso timer:sleep(Delay),
    {noreply, State#state{count = Count + 1}};

handle_cast({pi, Precision}, #state{count = Count, store = Store} = State) ->
    Pi = pi(Precision),
    {noreply, State#state{count = Count + 1, store = Store#{pi => Pi}}}.

pi(Precision) ->
    pi(Precision, 0, 1, 1).

pi(NeededPrecision, Result, Sign, Digit) ->
    Precision = 1 / Digit,
    if
        Precision > NeededPrecision ->
            pi(NeededPrecision, Result + (Sign * Precision), -Sign, Digit + 2);
        true ->
            Result
    end.

%%--------------------------------------------------------------------
%% Helpers
multi_sync(Pids) ->
    %% ask all workers at once, skipping monitoring
    Self = self(),
    Ref = erlang:make_ref(),
    [Pid ! {'$gen_call', {Self, Ref}, get_count} || Pid <- Pids],
    multi_receive(Ref, length(Pids)).

multi_receive(_Ref, 0) ->
    ok;
multi_receive(Ref, Count) ->
    receive
        {Ref, Int} when is_integer(Int) ->
            multi_receive(Ref, Count - 1)
    end.

%%--------------------------------------------------------------------
%% Metadata

%%--------------------------------------------------------------------
%% Load balancer correctness

lb() ->
    [{doc, "Tests weighted load balancer"}].

lb(Config) when is_list(Config) ->
    Scope = ?FUNCTION_NAME,
    WorkerCount = 4,
    SampleCount = 1000,
    %% start workers, when every next worker has +1 more capacity
    Workers = [
        begin
            {ok, S1} = gen_server:start_link(?MODULE, 1, []),
            _ = spg_plb:add(Scope, S1, Seq),
            {Seq, S1}
        end || Seq <- lists:seq(1, WorkerCount)],
    %% total weight
    TotalWeight = WorkerCount * (WorkerCount + 1) div 2,
    %% run samples
    _ = [spg_plb:cast(Scope, count) || _ <- lists:seq(1, SampleCount)],
    %% sync plb
    _ = [spg_plb:remove(Scope, W) || {_, W} <- Workers],
    %% ensure weights and total counts expected
    WeightedCounts = [begin
                  Count = gen_server:call(Pid, get_count),
                  ct:pal("Worker ~b/~b received ~b/~b", [Seq, Count, Count, SampleCount]),
                  {Seq, Count}
              end  || {Seq, Pid} <- Workers],
    {_, Counts} = lists:unzip(WeightedCounts),
    ?assertEqual(SampleCount, lists:sum(Counts)),
    [begin
        %% every server should have roughly it's share of samples
        Share = SampleCount * Seq div TotalWeight,
        %% ensure there is no 2x skew (isn't really expected from uniform PRNG)
        ?assert(Count div 2 < Share),
        ?assert(Count < Share * 2)
     end || {Seq, Count} <- WeightedCounts].

%%--------------------------------------------------------------------
%% Load balancer scalability

lb_cpu() ->
    [{doc, "Tests CPU-bound load"}].

lb_cpu(Config) when is_list(Config) ->
    PerProcessSamples = 400,
    Precision = 0.000001,
    Scope = ?FUNCTION_NAME,
    %% this process is high-pri
    _ = erlang:process_flag(priority, high),
    %% start "schedulers_online" workers, minus one for control worker
    WorkerCount = erlang:system_info(schedulers_online) - 1,
    ?assert(WorkerCount > 0),
    Workers = [start(0) || _ <- lists:seq(1, WorkerCount)],
    [spg_plb:add(Scope, W, PerProcessSamples) || W <- Workers],
    %% get total time to calculate overhead
    T1 = erlang:monotonic_time(),
    %% generate load
    [spg_plb:cast(Scope, {pi, Precision}) || _ <- lists:seq(1, PerProcessSamples * WorkerCount)],
    %% execute slow Samples in this process, also measuring time
    T2 = erlang:monotonic_time(),
    _ = [pi(Precision) || _ <- lists:seq(1, PerProcessSamples)],
    T3 = erlang:monotonic_time(),
    %% wait for all workers to complete
    multi_sync(Workers),
    %% get last timestamp
    T4 = erlang:monotonic_time(),
    _ = erlang:process_flag(priority, normal),
    %% calculate time passed
    TotalTime = erlang:convert_time_unit(T4 - T1, native, microsecond),
    NoCastTime = erlang:convert_time_unit(T4 - T2, native, microsecond),
    OneTime = erlang:convert_time_unit(T3 - T2, native, microsecond),
    WaitTime = erlang:convert_time_unit(T4 - T3, native, microsecond),
    %% overhead: ideally total time should be the same as single run time,
    %%  and overhead is the difference
    TotalOverhead = (TotalTime - OneTime) * 100 / OneTime,
    NoCastOverhead = (TotalTime - OneTime) * 100 / OneTime,
    ct:pal("Running ~b samples with ~b processes for ~f precision", [PerProcessSamples, WorkerCount, Precision]),
    ct:pal("Single: ~b ms, total ~b ms, overhead: ~.2f%", [OneTime div 1000, TotalTime div 1000,
        TotalOverhead]),
    ct:pal("Wait time: ~b ms, total (no cast) ~b ms, overhead: ~.2f%", [WaitTime div 1000, NoCastTime div 1000,
        NoCastOverhead]),
    %%
    {fail, ok}.