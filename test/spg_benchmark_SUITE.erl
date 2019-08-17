%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%%     spg (scalable groups simplified) benchmark. Targeted towards
%%%     fast 'get' operations, rather than leave/join, as latter
%%%     happens relatively rare.
%%% @end
%%% -------------------------------------------------------------------
-module(spg_benchmark_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases exports
-export([
    global/0, global/1,
    pg2/0, pg2/1,
    spg/0, spg/1,
    syn/0, syn/1,
    cpg/0, cpg/1
]).

%% Exports for RPC calls
-export([
    spawn/1,
    prepare/1,
    register/1,
    unregister/1,
    export_all/0
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    test_server_ctrl:kill_slavenodes().

all() ->
    % [global, pg2, spg, syn, cpg],
    [spg, syn].

%%--------------------------------------------------------------------
%% Benchmarking helpers

start_nodes(Count, Scope) ->
    NodeNames = [list_to_atom("node_" ++ integer_to_list(Seq)) || Seq <- lists:seq(1, Count - 1)],
    [spgt:spawn_node(Scope, Node) || Node <- NodeNames],
    AllNodes = [node() | nodes()],
    % fully connect mesh
    [
        begin
            [true = rpc:call(Node, net_kernel, connect_node, [N], 1000) || N <- AllNodes, N =/= node()],
            [pong = rpc:call(Node, net_adm, ping, [N], 1000) || N <- AllNodes, N =/= node()]
        end
        || Node <- AllNodes
    ].

% Launches a certain number of processes per node (for example, 25,000 processes per node).
% Registers these processes (25,000 processes per node), each with a globally unique Key.
% Waits for those Keys to be propagated to all the nodes.
% Unregisters all of these processes.
% Waits for those Keys to be removed from all the nodes.
% Re-registers all of the processes, to check for unwanted effects of subsequent add/remove operations.
% Again, waits for those Keys to be propagated to all the nodes.
% Kills all the processes (this time, without previously unregistering them).
% Waits for those Keys to be removed from all the nodes (to check for process monitoring).

global_prepare(_Name) ->
    ok.

global_register(Name, Pid) ->
    yes = global:register_name(Name, Pid).

global_unregister(Name, _Pid) ->
    global:unregister_name(Name).

global_whereis(Name) ->
    global:whereis_name(Name).

global_funs() ->
    {"global", fun global_prepare/1, fun global_register/2, fun global_unregister/2, fun global_whereis/1}.

pg2_prepare(Name) ->
    ok = pg2:create(Name).

pg2_register(Name, Pid) ->
    ok = pg2:join(Name, Pid).

pg2_unregister(Name, Pid) ->
    ok = pg2:leave(Name, Pid).

pg2_whereis(Name) ->
    case pg2:get_members(Name) of
        [Pid] ->
            Pid;
        _ ->
            undefined
    end.

pg2_funs() ->
    {"pg2", fun pg2_prepare/1, fun pg2_register/2, fun pg2_unregister/2, fun pg2_whereis/1}.

spg_prepare(_Name) ->
    ok.

spg_register(Name, Pid) ->
    ok = spg:join(Name, Pid).

spg_unregister(Name, Pid) ->
    ok = spg:leave(Name, Pid).

spg_whereis(Name) ->
    case spg:get_members(Name) of
        [Pid] ->
            Pid;
        _ ->
            undefined
    end.

spg_funs() ->
    {"spg", fun spg_prepare/1, fun spg_register/2, fun spg_unregister/2, fun spg_whereis/1}.

syn_prepare(_Name) ->
    ok.

syn_register(Name, Pid) ->
    ok = syn:register(Name, Pid).

syn_unregister(Name, _Pid) ->
    ok = syn:unregister(Name).

syn_whereis(Name) ->
    syn:find_by_key(Name).

syn_funs() ->
    {"syn", fun syn_prepare/1, fun syn_register/2, fun syn_unregister/2, fun syn_whereis/1}.

cpg_prepare(_Name) ->
    ok.

cpg_register(Name, Pid) ->
    ok = cpg:join(Name, Pid).

cpg_unregister(Name, Pid) ->
    ok = cpg:leave(Name, Pid).

cpg_whereis(Name) ->
    cpg:whereis_name(Name).

cpg_funs() ->
    {"cpg", fun cpg_prepare/1, fun cpg_register/2, fun cpg_unregister/2, fun cpg_whereis/1}.

%%--------------------------------------------------------------------
%% Runner

clean_pids_table(Tab, undefined) ->
    Tab = ets:new(Tab, [public, named_table, {heir, whereis(kernel_safe_sup), silent}]);
clean_pids_table(Tab, _Ref) ->
    ets:delete_all_objects(Tab).

-define (PID_TABLE, pid_table).

spawn(Count) ->
    clean_pids_table(?PID_TABLE, ets:whereis(?PID_TABLE)),
    Node = atom_to_list(node()) ++ "_",
    Pids = [{Node ++ integer_to_list(Seq), spgt:spawn()} || Seq <- lists:seq(1, Count)],
    true = ets:insert(?PID_TABLE, {pid, Pids}),
    Pids.

prepare(Fun) ->
    Procs = ets:lookup_element(?PID_TABLE, pid, 2),
    [Fun(Name) || {Name, _Pid} <- Procs],
    ok.

register(Fun) ->
    Procs = ets:lookup_element(?PID_TABLE, pid, 2),
    [Fun(Name, Pid) || {Name, Pid} <- Procs],
    ok.

unregister(Fun) ->
    Procs = ets:lookup_element(?PID_TABLE, pid, 2),
    [Fun(Name, Pid) || {Name, Pid} <- Procs].

wait(_Fun, []) ->
    ok;
wait(Fun, [{Name, Proc} | Tail] = Procs) ->
    case Fun(Name) of
        undefined ->
            timer:sleep(5),
            %ct:pal("Waiting for ~p (~p)", [Name, Proc]),
            %ct:pal("Global: ~200p", [global:registered_names()]),
            %[
            %    begin
            %        G = rpc:call(N, global, registered_names, [], 1000),
            %        ct:pal("~p: ~200p", [N, G])
            %    end
            %    || N <- nodes()
            %],
            wait(Fun, Procs);
        Proc ->
            wait(Fun, Tail)
    end.

wait_none(_Fun, []) ->
    ok;
wait_none(Fun, [{Name, Proc} | Tail] = Procs) ->
    case Fun(Name) of
        undefined ->
            wait_none(Fun, Tail);
        Proc ->
            timer:sleep(5),
            wait_none(Fun, Procs)
    end.


%% This is only needed to avoid constant nagging about unused functions.
export_all() ->
    pg2_funs(),
    spg_funs(),
    syn_funs(),
    global_funs().

timed_run(Fun, Args, Format, Name, ProcsTotal) when is_atom(Fun) ->
    {Time, _Result} = timer:tc(rpc, multicall, [?MODULE, Fun, Args]),
    ct:pal(Format, [Name, Time/1000000, ProcsTotal/Time*1000000]);

timed_run(Fun, Args, Format, Name, ProcsTotal) when is_function(Fun) ->
    {Time, _Result} = timer:tc(Fun, Args),
    ct:pal(Format, [Name, Time/1000000, ProcsTotal/Time*1000000]).

benchmark({Name, Prepare, Register, Unregister, Whereis}, ProcsPerNode) ->
    ProcsTotal = ProcsPerNode * (length(nodes()) + 1),
    {ProcsOfNodes, []} = rpc:multicall(?MODULE, spawn, [ProcsPerNode]),
    Procs = lists:concat(ProcsOfNodes),
    %
    % pg2: to make it fair, create all groups in advance
    {_, []} = rpc:multicall(?MODULE, prepare, [Prepare]),
    % register:
    timed_run(register, [Register], "~s: registered processes in ~p sec, at a rate of ~p/sec", Name, ProcsTotal),
    timed_run(fun wait/2, [Whereis, Procs], "~s: waiting registration propagation, ~p sec, at a rate of ~p/sec~n", Name, ProcsTotal),
    timed_run(unregister, [Unregister], "~s: unregistered processes in ~p sec, at a rate of ~p/sec", Name, ProcsTotal),
    timed_run(fun wait_none/2, [Whereis, Procs], "~s: waiting unregistration propagation, ~p sec, at a rate of ~p/sec~n", Name, ProcsTotal),
    timed_run(register, [Register], "~s: re-registered processes in ~p sec, at a rate of ~p/sec", Name, ProcsTotal),
    timed_run(fun wait/2, [Whereis, Procs], "~s: waiting re-registration propagation, ~p sec, at a rate of ~p/sec~n", Name, ProcsTotal),
    % kill & monitor
    [exit(Pid, kill) || {_, Pid} <- Procs],
    timed_run(fun wait_none/2, [Whereis, Procs], "~s: waiting for monitor down, ~p sec, at a rate of ~p/sec~n", Name, ProcsTotal).

%%--------------------------------------------------------------------
%% TEST CASES

%% Default number of nodes in a cluster (including self).
-define(NODE_COUNT, 16).

global() ->
    [{timetrap, {seconds, 20}}, {doc, "Benchmark for global"}].

global(_Config) ->
    start_nodes(?NODE_COUNT, undefined),
    benchmark(global_funs(), 2).

pg2() ->
    [{timetrap, {seconds, 120}}, {doc, "Benchmark for pg2"}].

pg2(_Config) ->
    start_nodes(?NODE_COUNT, undefined),
    benchmark(pg2_funs(), 10000).

spg() ->
    [{timetrap, {seconds, 120}}, {doc, "Benchmark for spg"}].

spg(_Config) ->
    start_nodes(?NODE_COUNT, ?FUNCTION_NAME),
    {ok, Pid} = spg:start_link(?FUNCTION_NAME),
    benchmark(spg_funs(), 10000),
    unlink(Pid),
    gen_server:stop(Pid).

syn() ->
    [{timetrap, {seconds, 120}}, {doc, "Benchmark for syn"}].

syn(_Config) ->
    start_nodes(?NODE_COUNT, ?FUNCTION_NAME),
    AllNodes = [node() | nodes()],
    % add syn to code path
    Path = filename:dirname(code:which(syn)),
    ?assertEqual({lists:duplicate(?NODE_COUNT, true), []}, rpc:multicall(code, add_path, [Path])),

    % start SYN everywhere
    [begin
         ok = rpc:call(Node, syn, start, [], 5000),
         ok = rpc:call(Node, syn, init, [], 15000)
     end || Node <- AllNodes],
    benchmark(syn_funs(), 10000),
    ?assertEqual({lists:duplicate(?NODE_COUNT, ok), []}, rpc:multicall(syn, stop, [])),
    ?assertEqual({lists:duplicate(?NODE_COUNT, stopped), []}, rpc:multicall(mnesia, stop, [])).

cpg() ->
    [{timetrap, {seconds, 120}}, {doc, "Benchmark for cpg"}].

cpg(_Config) ->
    start_nodes(?NODE_COUNT, ?FUNCTION_NAME),
    Paths = [filename:dirname(code:which(App)) || App <- [cpg, quickrand, trie]],
    ?assertEqual({lists:duplicate(?NODE_COUNT, ok), []}, rpc:multicall(code, add_paths, [Paths])),
    ?assertMatch({_, []}, rpc:multicall(application, ensure_all_started, [cpg])),
    benchmark(cpg_funs(), 1000),
    ?assertEqual({lists:duplicate(?NODE_COUNT, true), []}, rpc:multicall(application, stop, [cpg])).
