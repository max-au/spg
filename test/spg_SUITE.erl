%%-------------------------------------------------------------------
%% @author Maxim Fedorov <maximfca@gmail.com>
%%     Process Groups smoke test.
-module(spg_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    stop_proc/1
]).

%% Test cases exports
-export([
    app/0, app/1,
    spg/0, spg/1,
    errors/0, errors/1,
    leave_exit_race/0, leave_exit_race/1,
    dyn_distribution/0, dyn_distribution/1,
    process_owner_check/0, process_owner_check/1,
    overlay_missing/0, overlay_missing/1,
    single/0, single/1,
    two/1,
    empty_group_by_remote_leave/0, empty_group_by_remote_leave/1,
    thundering_herd/0, thundering_herd/1,
    initial/1,
    netsplit/1,
    trisplit/1,
    foursplit/1,
    exchange/1,
    nolocal/1,
    double/1,
    scope_restart/1,
    missing_scope_join/1,
    disconnected_start/1,
    forced_sync/0, forced_sync/1,
    group_leave/1,
    monitor_nonempty_scope/0, monitor_nonempty_scope/1,
    monitor_scope/0, monitor_scope/1,
    monitor/1,
    protocol_upgrade/1
]).

-export([
    control/1,
    controller/3,
    ensure_peers_info/2
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    case erlang:is_alive() of
        false ->
            %% verify epmd running (otherwise next call fails)
            (erl_epmd:names() =:= {error, address}) andalso
                begin ([] = os:cmd("epmd -daemon")),
                timer:sleep(500) %% this is needed to let some time for daemon to actually start
            end,
            %% verify that epmd indeed started
            {ok, _} = erl_epmd:names(),
            %% start a random node name
            NodeName = list_to_atom(lists:concat([atom_to_list(?MODULE), "_", os:getpid()])),
            {ok, Pid} = net_kernel:start([NodeName, shortnames]),
            [{distribution, Pid} | Config];
        true ->
            Config
    end.

end_per_suite(Config) ->
    is_pid(proplists:get_value(distribution, Config)) andalso net_kernel:stop().

init_per_testcase(app, Config) ->
    Config;
init_per_testcase(TestCase, Config) ->
    {ok, Pid} = spg:start_link(TestCase),
    trace_start(TestCase, Config, Pid).

end_per_testcase(app, _Config) ->
    application:stop(spg),
    ok;
end_per_testcase(TestCase, Config) ->
    gen_server:stop(TestCase),
    trace_end(Config),
    ok.

trace_start(forced_sync, Config, Tracee) ->
    Tracer = spawn_link(fun() -> tracer() end),
    1 = erlang:trace(Tracee, true, ['receive', {tracer, Tracer}, timestamp]),
    [{tracer, Tracer} | Config];
trace_start(_, Config, _) ->
    Config.

trace_end(Config) ->
    case proplists:get_value(tracer, Config) of
        undefined -> ok;
        Tracer ->
            Mon = erlang:monitor(process, Tracer),
            Tracer ! flush,
            normal = receive
                         {'DOWN', Mon, process, Tracer, R} -> R
                     end
    end.

tracer() ->
    receive flush -> ok end,
    tracer_flush().

tracer_flush() ->
    receive _M ->
        tracer_flush()
    after 0 ->
        ok
    end.

all() ->
    [app, dyn_distribution, {group, basic}, {group, cluster}, {group, performance}, {group, monitor}].

groups() ->
    [
        {basic, [parallel], [errors, spg, leave_exit_race, single, process_owner_check, overlay_missing]},
        {performance, [sequential], [thundering_herd]},
        {cluster, [parallel], [two, initial, netsplit, trisplit, foursplit,
            exchange, nolocal, double, scope_restart, missing_scope_join, empty_group_by_remote_leave,
            disconnected_start, forced_sync, group_leave]},
        {monitor, [parallel], [monitor_scope, monitor]}
    ].

%%--------------------------------------------------------------------
%% TEST CASES

spg() ->
    [{doc, "This test must be names spg, to stay inline with default scope"}].

spg(_Config) ->
    ?assertNotEqual(undefined, whereis(?FUNCTION_NAME)), %% ensure scope was started
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, self())),
    ?assertEqual([self()], spg:get_local_members(?FUNCTION_NAME)),
    ?assertEqual([?FUNCTION_NAME], spg:which_groups()),
    ?assertEqual([?FUNCTION_NAME], spg:which_local_groups()),
    ?assertEqual(ok, spg:leave(?FUNCTION_NAME, self())),
    ?assertEqual([], spg:get_members(?FUNCTION_NAME)),
    ?assertEqual([], spg:which_groups(?FUNCTION_NAME)),
    ?assertEqual([], spg:which_local_groups(?FUNCTION_NAME)).

errors() ->
    [{doc, "Tests that errors are handled as expected, for example spg server crashes when it needs to"}].

errors(_Config) ->
    %% kill with 'info' and 'cast'
    ?assertException(error, badarg, spg:handle_info(garbage, garbage)),
    ?assertException(error, badarg, spg:handle_cast(garbage, garbage)),
    %% kill with call
    {ok, _Pid} = spg:start(second),
    ?assertException(exit, {{badarg, _}, _}, gen_server:call(second, garbage, infinity)).


app() ->
    [{doc, "Tests application start/stop functioning, supervision & scopes"}].

app(_Config) ->
    {ok, Apps} = application:ensure_all_started(spg),
    ?assertNotEqual(undefined, whereis(spg)),
    ?assertNotEqual(undefined, ets:whereis(spg)),
    ok = application:stop(spg),
    ?assertEqual(undefined, whereis(spg)),
    ?assertEqual(undefined, ets:whereis(spg)),
    %
    application:set_env(spg, scopes, [?FUNCTION_NAME, two]),
    {ok, Apps} = application:ensure_all_started(spg),
    ?assertNotEqual(undefined, whereis(?FUNCTION_NAME)),
    ?assertNotEqual(undefined, whereis(two)),
    ?assertNotEqual(undefined, ets:whereis(?FUNCTION_NAME)),
    ?assertNotEqual(undefined, ets:whereis(two)),
    ok = application:stop(spg),
    ok = application:unload(spg),
    ?assertEqual(undefined, whereis(?FUNCTION_NAME)),
    ?assertEqual(undefined, whereis(two)).

leave_exit_race() ->
    [{doc, "Tests that spg correctly handles situation when leave and 'DOWN' messages are both in spg queue"}].

leave_exit_race(Config) when is_list(Config) ->
    process_flag(priority, high),
    [
        begin
            Pid = spawn(fun () -> ok end),
            spg:join(leave_exit_race, test, Pid),
            spg:leave(leave_exit_race, test, Pid)
        end
        || _ <- lists:seq(1, 100)].

single() ->
    [{doc, "Tests single node groups"}, {timetrap, {seconds, 5}}].

single(Config) when is_list(Config) ->
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, self())),
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, [self(), self()])),
    ?assertEqual([self(), self(), self()], spg:get_local_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    ?assertEqual([self(), self(), self()], spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    ?assertEqual(not_joined, spg:leave(?FUNCTION_NAME, '$missing$', self())),
    ?assertEqual(ok, spg:leave(?FUNCTION_NAME, ?FUNCTION_NAME, [self(), self()])),
    ?assertEqual(ok, spg:leave(?FUNCTION_NAME, ?FUNCTION_NAME, self())),
    ?assertEqual([], spg:which_groups(?FUNCTION_NAME)),
    ?assertEqual([], spg:get_local_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    ?assertEqual([], spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    %% double
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, self())),
    Pid = erlang:spawn(forever()),
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, Pid)),
    Expected = lists:sort([Pid, self()]),
    ?assertEqual(Expected, lists:sort(spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME))),
    ?assertEqual(Expected, lists:sort(spg:get_local_members(?FUNCTION_NAME, ?FUNCTION_NAME))),
    stop_proc(Pid),
    sync(?FUNCTION_NAME),
    ?assertEqual([self()], spg:get_local_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    ?assertEqual(ok, spg:leave(?FUNCTION_NAME, ?FUNCTION_NAME, self())),
    ok.

dyn_distribution() ->
    [{doc, "Tests that local node when distribution is started dynamically is not treated as remote node"}].

dyn_distribution(Config) when is_list(Config) ->
    %% When distribution is started or stopped dynamically,
    %%  there is a nodeup/nodedown message delivered to pg
    %% It is possible but non-trivial to simulate this
    %%  behaviour with starting slave nodes being not
    %%  distributed, and calling net_kernel:start/1, however
    %%  the effect is still the same as simply sending nodeup,
    %%  which is also documented.
    ?FUNCTION_NAME ! {nodeup, node()},
    %%
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, self())),
    ?assertEqual([self()], spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    ok.

process_owner_check() ->
    [{doc, "Tests that process owner is local node"}].

process_owner_check(Config) when is_list(Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    %% spawn remote process
    LocalPid = erlang:spawn(forever()),
    RemotePid = erlang:spawn(Node, forever()),
    %% check they can't be joined locally
    ?assertException(error, {nolocal, _}, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, RemotePid)),
    ?assertException(error, {nolocal, _}, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, [RemotePid, RemotePid])),
    ?assertException(error, {nolocal, _}, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, [LocalPid, RemotePid])),
    %% check that non-pid also triggers error
    ?assertException(error, function_clause, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, undefined)),
    ?assertException(error, {nolocal, _}, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, [undefined])),
    %% stop the peer
    stop_node(Peer, Node),
    ok.

overlay_missing() ->
    [{doc, "Tests that scope process that is not a part of overlay network does not change state"}].

overlay_missing(_Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    %% join self (sanity check)
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, group, self())),
    %% remember pid from remote
    PgPid = rpc:call(Node, erlang, whereis, [?FUNCTION_NAME]),
    RemotePid = erlang:spawn(Node, forever()),
    %% stop remote scope
    gen_server:stop(PgPid),
    %% craft white-box request: ensure it's rejected
    ?FUNCTION_NAME ! {join, PgPid, group, RemotePid},
    %% rejected!
    ?assertEqual([self()], spg:get_members(?FUNCTION_NAME, group)),
    %% ... reject leave too
    ?FUNCTION_NAME ! {leave, PgPid, RemotePid, [group]},
    ?assertEqual([self()], spg:get_members(?FUNCTION_NAME, group)),
    %% join many times on remote
    %RemotePids = [erlang:spawn(TwoPeer, forever()) || _ <- lists:seq(1, 1024)],
    %?assertEqual(ok, rpc:call(TwoPeer, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, Pid2])),
    %% check they can't be joined locally
    %?assertException(error, {nolocal, _}, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, RemotePid)),
    %?assertException(error, {nolocal, _}, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, [RemotePid, RemotePid])),
    %?assertException(error, {nolocal, _}, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, [LocalPid, RemotePid])),
    %% check that non-pid also triggers error
    %?assertException(error, function_clause, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, undefined)),
    %?assertException(error, {nolocal, _}, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, [undefined])),
    %% stop the peer
    stop_node(Peer, Node).


two(Config) when is_list(Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    Pid = erlang:spawn(forever()),
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, Pid)),
    ?assertEqual([Pid], spg:get_local_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    %% first RPC must be serialised 3 times
    sync({?FUNCTION_NAME, Node}),
    sync(?FUNCTION_NAME),
    sync({?FUNCTION_NAME, Node}),
    ?assertEqual([Pid], rpc:call(Node, spg, get_members, [?FUNCTION_NAME, ?FUNCTION_NAME])),
    ?assertEqual([], rpc:call(Node, spg, get_local_members, [?FUNCTION_NAME, ?FUNCTION_NAME])),
    stop_proc(Pid),
    %% again, must be serialised
    sync(?FUNCTION_NAME),
    sync({?FUNCTION_NAME, Node}),
    ?assertEqual([], spg:get_local_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    ?assertEqual([], rpc:call(Node, spg, get_members, [?FUNCTION_NAME, ?FUNCTION_NAME])),

    Pid2 = erlang:spawn(Node, forever()),
    Pid3 = erlang:spawn(Node, forever()),
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, Pid2])),
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, Pid3])),
    %% serialise through the *other* node
    sync_via({?FUNCTION_NAME, Node}, ?FUNCTION_NAME),
    ?assertEqual(lists:sort([Pid2, Pid3]),
        lists:sort(spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME))),
    %% stop the peer
    stop_node(Peer, Node),
    %% hope that 'nodedown' comes before we route our request
    sync(?FUNCTION_NAME),
    ok.

empty_group_by_remote_leave() ->
    [{doc, "Empty group should be deleted from nodes."}].

empty_group_by_remote_leave(Config) when is_list(Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    RemoteNode = rpc:call(Node, erlang, whereis, [?FUNCTION_NAME]),
    RemotePid = erlang:spawn(Node, forever()),
    % remote join
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, RemotePid])),
    sync_via({?FUNCTION_NAME, Node}, ?FUNCTION_NAME),
    ?assertEqual([RemotePid], spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    % inspecting internal state is not best practice, but there's no other way to check if the state is correct.
    {_, RemoteMap} = maps:get(RemoteNode, element(4, sys:get_state(?FUNCTION_NAME))),
    ?assertEqual(#{?FUNCTION_NAME => [RemotePid]}, RemoteMap),
    % remote leave
    ?assertEqual(ok, rpc:call(Node, spg, leave, [?FUNCTION_NAME, ?FUNCTION_NAME, RemotePid])),
    sync_via({?FUNCTION_NAME, Node}, ?FUNCTION_NAME),
    ?assertEqual([], spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    {_, NewRemoteMap} = maps:get(RemoteNode, element(4, sys:get_state(?FUNCTION_NAME))),
    % empty group should be deleted.
    ?assertEqual(#{}, NewRemoteMap),

    %% another variant of emptying a group remotely: join([Pi1, Pid2]) and leave ([Pid2, Pid1])
    RemotePid2 = erlang:spawn(Node, forever()),
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, [RemotePid, RemotePid2]])),
    sync_via({?FUNCTION_NAME, Node}, ?FUNCTION_NAME),
    ?assertEqual([RemotePid, RemotePid2], spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    %% now leave
    ?assertEqual(ok, rpc:call(Node, spg, leave, [?FUNCTION_NAME, ?FUNCTION_NAME, [RemotePid2, RemotePid]])),
    sync_via({?FUNCTION_NAME, Node}, ?FUNCTION_NAME),
    ?assertEqual([], spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    {_, NewRemoteMap} = maps:get(RemoteNode, element(4, sys:get_state(?FUNCTION_NAME))),
    stop_node(Peer, Node),
    ok.

thundering_herd() ->
    [{doc, "Thousands of overlay network nodes sending sync to us, and we time out!"}, {timetrap, {seconds, 5}}].

thundering_herd(Config) when is_list(Config) ->
    GroupCount = 10000,
    SyncCount = 2000,
    %% make up a large amount of groups
    [spg:join(?FUNCTION_NAME, {group, Seq}, self()) || Seq <- lists:seq(1, GroupCount)],
    %% initiate a few syncs - and those are really slow...
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    PeerPid = erlang:spawn(Node, forever()),
    PeerPg = rpc:call(Node, erlang, whereis, [?FUNCTION_NAME], 1000),
    %% WARNING: code below acts for white-box! %% WARNING
    FakeSync = [{{group, 1}, [PeerPid, PeerPid]}],
    [gen_server:cast(?FUNCTION_NAME, {sync, PeerPg, FakeSync}) || _ <- lists:seq(1, SyncCount)],
    %% next call must not timetrap, otherwise test fails
    spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, self()),
    stop_node(Peer, Node).

initial(Config) when is_list(Config) ->
    Pid = erlang:spawn(forever()),
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, Pid)),
    ?assertEqual([Pid], spg:get_local_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    %% first sync makes the peer node to process 'nodeup' (and send discover)
    sync({?FUNCTION_NAME, Node}),
    %% second sync makes origin node pg to reply to discover'
    sync(?FUNCTION_NAME),
    %% third sync makes peer node to finish processing 'exchange'
    sync({?FUNCTION_NAME, Node}),
    ?assertEqual([Pid], rpc:call(Node, spg, get_members, [?FUNCTION_NAME, ?FUNCTION_NAME])),

    ?assertEqual([], rpc:call(Node, spg, get_local_members, [?FUNCTION_NAME, ?FUNCTION_NAME])),
    stop_proc(Pid),
    sync({?FUNCTION_NAME, Node}),
    ?assertEqual([], rpc:call(Node, spg, get_members, [?FUNCTION_NAME, ?FUNCTION_NAME])),
    stop_node(Peer, Node),
    ok.

netsplit(Config) when is_list(Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    ?assertEqual(Node, rpc(Peer, erlang, node, [])), %% just to test RPC
    RemoteOldPid = erlang:spawn(Node, forever()),
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, '$invisible', RemoteOldPid])),
    sync_via({?FUNCTION_NAME, Node}, ?FUNCTION_NAME),
    ?assertEqual([RemoteOldPid], spg:get_members(?FUNCTION_NAME, '$invisible')),
    %% hohoho, partition!
    disconnect_nodes([Node]),
    ?assertEqual(Node, rpc(Peer, erlang, node, [])), %% just to ensure RPC still works
    RemotePid = rpc(Peer, erlang, spawn, [forever()]),
    ?assertEqual([], rpc(Peer, erlang, nodes, [])),
    ?assertNot(lists:member(Node, nodes())), %% should be no nodes in the cluster
    ?assertEqual(ok, rpc(Peer, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, RemotePid])), %% join - in a partition!

    ?assertEqual(ok, rpc(Peer, spg, leave, [?FUNCTION_NAME, '$invisible', RemoteOldPid])),
    ?assertEqual(ok, rpc(Peer, spg, join, [?FUNCTION_NAME, '$visible', RemoteOldPid])),
    ?assertEqual([RemoteOldPid], rpc(Peer, spg, get_local_members, [?FUNCTION_NAME, '$visible'])),
    %% join locally too
    LocalPid = erlang:spawn(forever()),
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, LocalPid)),

    ?assertNot(lists:member(Node, nodes())), %% should be no nodes in the cluster

    %% now ensure sync happened
    PgPid = whereis(?FUNCTION_NAME),
    1 = erlang:trace(PgPid, true, ['receive']),
    pong = net_adm:ping(Node),
    receive
        {trace, PgPid, 'receive', {nodeup, Node}} -> ok
    end,
    1 = erlang:trace(PgPid, false, ['receive']),

    %% now ensure sync happened
    sync_via(?FUNCTION_NAME, {?FUNCTION_NAME, Node}),
    sync_via({?FUNCTION_NAME, Node}, ?FUNCTION_NAME),
    ?assertEqual(lists:sort([RemotePid, LocalPid]),
        lists:sort(rpc:call(Node, spg, get_members, [?FUNCTION_NAME, ?FUNCTION_NAME]))),
    ok.

trisplit(Config) when is_list(Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    _PeerPid1 = erlang:spawn(Node, forever()),
    PeerPid2 = erlang:spawn(Node, forever()),
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, three, PeerPid2])),
    disconnect_nodes([Node]),
    ?assertEqual(true, net_kernel:connect_node(Node)),
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, one, PeerPid2])),
    %% now ensure sync happened
    {Peer2, Node2} = spawn_node(?FUNCTION_NAME),
    ?assertEqual(true, rpc:call(Node2, net_kernel, connect_node, [Node])),
    ?assertEqual(lists:sort([node(), Node]), lists:sort(rpc:call(Node2, erlang, nodes, []))),
    ok = rpc:call(Node2, ?MODULE, ensure_peers_info, [?FUNCTION_NAME, [node(), Node]]),
    ?assertEqual([PeerPid2], rpc:call(Node2, spg, get_members, [?FUNCTION_NAME, one])),
    stop_node(Peer, Node),
    stop_node(Peer2, Node2),
    ok.

foursplit(Config) when is_list(Config) ->
    Pid = erlang:spawn(forever()),
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, one, Pid)),
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, two, Pid)),
    PeerPid1 = erlang:spawn(Node, forever()),
    ?assertEqual(ok, spg:leave(?FUNCTION_NAME, one, Pid)),
    ?assertEqual(not_joined, spg:leave(?FUNCTION_NAME, three, Pid)),
    disconnect_nodes([Node]),
    ?assertEqual(ok, rpc(Peer, ?MODULE, stop_proc, [PeerPid1])),
    ?assertEqual(not_joined, spg:leave(?FUNCTION_NAME, three, Pid)),
    ?assertEqual(true, net_kernel:connect_node(Node)),
    ?assertEqual([], spg:get_members(?FUNCTION_NAME, one)),
    ?assertEqual([], rpc(Peer, spg, get_members, [?FUNCTION_NAME, one])),
    stop_node(Peer, Node),
    ok.

exchange(Config) when is_list(Config) ->
    {Peer1, Node1} = spawn_node(?FUNCTION_NAME),
    {Peer2, Node2} = spawn_node(?FUNCTION_NAME),
    Pids10 = [rpc(Peer1, erlang, spawn, [forever()]) || _ <- lists:seq(1, 10)],
    Pids2 = [rpc(Peer2, erlang, spawn, [forever()]) || _ <- lists:seq(1, 10)],
    Pids11 = [rpc(Peer1, erlang, spawn, [forever()]) || _ <- lists:seq(1, 10)],
    %% kill first 3 pids from node1
    {PidsToKill, Pids1} = lists:split(3, Pids10),

    ?assertEqual(ok, rpc(Peer1, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, Pids10])),
    sync({?FUNCTION_NAME, Node1}), %% Join broadcast have reached local
    sync(?FUNCTION_NAME), %% Join broadcast has been processed by local
    ?assertEqual(lists:sort(Pids10), lists:sort(spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME))),
    [rpc(Peer1, ?MODULE, stop_proc, [Pid]) || Pid <- PidsToKill],
    sync(?FUNCTION_NAME),
    sync({?FUNCTION_NAME, Node1}),

    Pids = lists:sort(Pids1 ++ Pids2 ++ Pids11),
    ?assert(lists:all(fun erlang:is_pid/1, Pids)),

    disconnect_nodes([Node1, Node2]),

    sync(?FUNCTION_NAME), %% Processed nodedowns...
    ?assertEqual([], lists:sort(spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME))),

    [?assertEqual(ok, rpc(Peer2, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, Pid])) || Pid <- Pids2],
    [?assertEqual(ok, rpc(Peer1, spg, join, [?FUNCTION_NAME, second, Pid])) || Pid <- Pids11],
    ?assertEqual(ok, rpc(Peer1, spg, join, [?FUNCTION_NAME, third, Pids11])),
    %% rejoin
    ?assertEqual(true, net_kernel:connect_node(Node1)),
    ?assertEqual(true, net_kernel:connect_node(Node2)),
    %% need to sleep longer to ensure both nodes made the exchange
    ensure_peers_info(?FUNCTION_NAME, [Node1, Node2]),
    ?assertEqual(Pids, lists:sort(spg:get_members(?FUNCTION_NAME, second) ++ spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME))),
    ?assertEqual(lists:sort(Pids11), lists:sort(spg:get_members(?FUNCTION_NAME, third))),

    {Left, Stay} = lists:split(3, Pids11),
    ?assertEqual(ok, rpc(Peer1, spg, leave, [?FUNCTION_NAME, third, Left])),
    sync({?FUNCTION_NAME, Node1}),
    sync(?FUNCTION_NAME),
    ?assertEqual(lists:sort(Stay), lists:sort(spg:get_members(?FUNCTION_NAME, third))),
    ?assertEqual(not_joined, rpc(Peer1, spg, leave, [?FUNCTION_NAME, left, Stay])),
    ?assertEqual(ok, rpc(Peer1, spg, leave, [?FUNCTION_NAME, third, Stay])),
    sync({?FUNCTION_NAME, Node1}),
    sync(?FUNCTION_NAME),
    ?assertEqual([], lists:sort(spg:get_members(?FUNCTION_NAME, third))),
    sync({?FUNCTION_NAME, Node1}),
    sync(?FUNCTION_NAME),

    stop_node(Peer1, Node1),
    stop_node(Peer2, Node2),
    ok.

nolocal(Config) when is_list(Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    RemotePid = erlang:spawn(Node, forever()),
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, RemotePid])),
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, RemotePid])),
    ?assertEqual([], spg:get_local_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    stop_node(Peer, Node),
    ok.

double(Config) when is_list(Config) ->
    Pid = erlang:spawn(forever()),
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, Pid)),
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, [Pid])),
    ?assertEqual([Pid, Pid], spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    sync({?FUNCTION_NAME, Node}),
    sync_via(?FUNCTION_NAME, {?FUNCTION_NAME, Node}),
    ?assertEqual([Pid, Pid], rpc:call(Node, spg, get_members, [?FUNCTION_NAME, ?FUNCTION_NAME])),
    stop_node(Peer, Node),
    ok.

scope_restart(Config) when is_list(Config) ->
    Pid = erlang:spawn(forever()),
    ?assertEqual(ok, spg:join(?FUNCTION_NAME, ?FUNCTION_NAME, [Pid, Pid])),
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    RemotePid = erlang:spawn(Node, forever()),
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, RemotePid])),
    sync({?FUNCTION_NAME, Node}),
    ?assertEqual(lists:sort([RemotePid, Pid, Pid]), lists:sort(spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME))),
    %% stop scope locally, and restart
    gen_server:stop(?FUNCTION_NAME),
    spg:start(?FUNCTION_NAME),
    %% ensure remote pids joined, local are missing
    sync(?FUNCTION_NAME),
    sync({?FUNCTION_NAME, Node}),
    sync(?FUNCTION_NAME),
    ?assertEqual([RemotePid], spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME)),
    stop_node(Peer, Node),
    ok.

missing_scope_join(Config) when is_list(Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    ?assertEqual(ok, rpc:call(Node, gen_server, stop, [?FUNCTION_NAME])),
    RemotePid = erlang:spawn(Node, forever()),
    ?assertMatch({badrpc, {'EXIT', {noproc, _}}}, rpc:call(Node, spg, join, [?FUNCTION_NAME, ?FUNCTION_NAME, RemotePid])),
    ?assertMatch({badrpc, {'EXIT', {noproc, _}}}, rpc:call(Node, spg, leave, [?FUNCTION_NAME, ?FUNCTION_NAME, RemotePid])),
    stop_node(Peer, Node),
    ok.

disconnected_start(Config) when is_list(Config) ->
    case test_server:is_cover() of
        true ->
            {skip, "Cover is running"};
        false ->
            disconnected_start_test(Config)
    end.

disconnected_start_test(Config) when is_list(Config) ->
    {Peer, Node} = spawn_disconnected_node(?FUNCTION_NAME, ?FUNCTION_NAME),
    ?assertNot(lists:member(Node, nodes())),
    ?assertEqual(ok, rpc(Peer, gen_server, stop, [?FUNCTION_NAME])),
    ?assertMatch({ok, _Pid}, rpc(Peer, spg, start,[?FUNCTION_NAME])),
    ?assertEqual(ok, rpc(Peer, gen_server, stop, [?FUNCTION_NAME])),
    RemotePid = rpc(Peer, erlang, spawn, [forever()]),
    ?assert(is_pid(RemotePid)),
    stop_node(Peer, Node),
    ok.

forced_sync() ->
    [{doc, "This test was added when lookup_element was erroneously used instead of lookup, crashing pg with badmatch, and it tests rare out-of-order sync operations"}].

forced_sync(Config) when is_list(Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    Pid = erlang:spawn(forever()),
    RemotePid = erlang:spawn(Node, forever()),
    Expected = lists:sort([Pid, RemotePid]),
    spg:join(?FUNCTION_NAME, one, Pid),

    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, one, RemotePid])),
    RemoteScopePid = rpc:call(Node, erlang, whereis, [?FUNCTION_NAME]),
    ?assert(is_pid(RemoteScopePid)),
    %% hohoho, partition!
    disconnect_nodes([Node]),
    ?assertEqual(true, net_kernel:connect_node(Node)),
    ensure_peers_info(?FUNCTION_NAME, [Node]),
    ?assertEqual(Expected, lists:sort(spg:get_members(?FUNCTION_NAME, one))),

    %% Do extra sync to make sure any redundant sync message has arrived
    %% before we send our fake sync message below.
    sync_via({?FUNCTION_NAME, Node}, ?FUNCTION_NAME),

    %% WARNING: this code uses pg as white-box, exploiting internals,
    %%  only to simulate broken 'sync'
    %% Fake Groups: one should disappear, one should be replaced, one stays
    %% This tests handle_sync function.
    FakeGroups = [{one, [RemotePid, RemotePid]}, {?FUNCTION_NAME, [RemotePid, RemotePid]}],
    gen_server:cast(?FUNCTION_NAME, {sync, RemoteScopePid, FakeGroups}),
    %% ensure it is broken well enough
    sync(?FUNCTION_NAME),
    ?assertEqual(lists:sort([RemotePid, RemotePid]), lists:sort(spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME))),
    ?assertEqual(lists:sort([RemotePid, RemotePid, Pid]), lists:sort(spg:get_members(?FUNCTION_NAME, one))),
    %% simulate force-sync via 'discover' - ask peer to send sync to us
    {?FUNCTION_NAME, Node} ! {discover, whereis(?FUNCTION_NAME)},
    sync({?FUNCTION_NAME, Node}),
    sync(?FUNCTION_NAME),
    ?assertEqual(Expected, lists:sort(spg:get_members(?FUNCTION_NAME, one))),
    ?assertEqual([], lists:sort(spg:get_members(?FUNCTION_NAME, ?FUNCTION_NAME))),
    %% and simulate extra sync
    sync({?FUNCTION_NAME, Node}),
    sync(?FUNCTION_NAME),
    ?assertEqual(Expected, lists:sort(spg:get_members(?FUNCTION_NAME, one))),

    stop_node(Peer, Node),
    ok.

group_leave(Config) when is_list(Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    RemotePid = erlang:spawn(Node, forever()),
    Total = lists:duplicate(16, RemotePid),
    {Left, Remain} = lists:split(4, Total),
    %% join 16 times!
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, two, Total])),
    ?assertEqual(ok, rpc:call(Node, spg, leave, [?FUNCTION_NAME, two, Left])),

    sync({?FUNCTION_NAME, Node}),
    sync(?FUNCTION_NAME),
    ?assertEqual(Remain, spg:get_members(?FUNCTION_NAME, two)),
    PgPid = whereis(?FUNCTION_NAME),
    1 = erlang:trace(PgPid, true, ['receive']),
    stop_node(Peer, Node),
    receive
        {trace, PgPid, 'receive', {nodedown, Node}} -> ok
    end,
    1 = erlang:trace(PgPid, false, ['receive']),
    sync(?FUNCTION_NAME),
    ?assertEqual([], spg:get_members(?FUNCTION_NAME, two)),
    ok.

monitor_nonempty_scope() ->
    [{doc, "Ensure that monitor_scope returns full map of groups in the scope"}].

monitor_nonempty_scope(Config) when is_list(Config) ->
    {Peer, Node} = spawn_node(?FUNCTION_NAME),
    Pid = erlang:spawn_link(forever()),
    RemotePid = erlang:spawn(Node, forever()),
    Expected = lists:sort([Pid, RemotePid]),
    spg:join(?FUNCTION_NAME, one, Pid),
    ?assertEqual(ok, rpc:call(Node, spg, join, [?FUNCTION_NAME, one, RemotePid])),
    %% Ensure that initial monitoring request returns current map of groups to pids
    {Ref, #{one := Actual} = FullScope} = spg:monitor_scope(?FUNCTION_NAME),
    ?assertEqual(Expected, Actual),
    %% just in case - check there are no extra groups in that scope
    ?assertEqual(1, map_size(FullScope)),
    spg:demonitor(?FUNCTION_NAME, Ref),
    %% re-check
    {_Ref, FullScope} = spg:monitor_scope(?FUNCTION_NAME),
    stop_node(Peer, Node),
    exit(Pid, normal).

monitor_scope() ->
    [{doc, "Tests monitor_scope/1 and demonitor/2"}].

monitor_scope(Config) when is_list(Config) ->
    %% ensure that demonitoring returns 'false' when monitor is not installed
    ?assertEqual(false, spg:demonitor(?FUNCTION_NAME, erlang:make_ref())),
    InitialMonitor = fun (Scope) -> {Ref, #{}} = spg:monitor_scope(Scope), Ref end,
    SecondMonitor = fun (Scope, Group, Control) -> {Ref, #{Group := [Control]}} = spg:monitor_scope(Scope), Ref end,
    %% WHITE BOX: knowing pg state internals - only the original monitor should stay
    DownMonitor = fun (Scope, Ref, Self) ->
        {state, _, _, _, ScopeMonitors, _, _} = sys:get_state(Scope),
        ?assertEqual(#{Ref => Self}, ScopeMonitors, "pg did not remove DOWNed scope monitor")
    end,
    monitor_test_impl(?FUNCTION_NAME, ?FUNCTION_ARITY, InitialMonitor, SecondMonitor, DownMonitor).

monitor(Config) when is_list(Config) ->
    ExpectedGroup = {?FUNCTION_NAME, ?FUNCTION_ARITY},
    InitialMonitor = fun (Scope) -> {Ref, []} = spg:monitor(Scope, ExpectedGroup), Ref end,
    SecondMonitor = fun (Scope, Group, Control) ->
        {Ref, [Control]} = spg:monitor(Scope, Group), Ref end,
    DownMonitor = fun (Scope, Ref, Self) ->
        {state, _, _, _, _, GM, MG} = sys:get_state(Scope),
        ?assertEqual(#{Ref => {Self, ExpectedGroup}}, GM, "pg did not remove DOWNed group monitor"),
        ?assertEqual(#{ExpectedGroup => [{Self, Ref}]}, MG, "pg did not remove DOWNed group")
    end,
    monitor_test_impl(?FUNCTION_NAME, ExpectedGroup, InitialMonitor, SecondMonitor, DownMonitor).

monitor_test_impl(Scope, Group, InitialMonitor, SecondMonitor, DownMonitor) ->
    Self = self(),
    Ref = InitialMonitor(Scope),
    %% local join
    ?assertEqual(ok, spg:join(Scope, Group, Self)),
    wait_message(Ref, join, Group, [Self], "Local"),
    %% start second monitor (which has 1 local pid at the start)
    ExtraMonitor = spawn_link(fun() -> second_monitor(Scope, Group, Self, SecondMonitor) end),
    Ref2 = receive {ExtraMonitor, SecondRef} -> SecondRef end,
    %% start a remote node, and a remote monitor
    {Peer, Node} = spawn_node(Scope),
    ScopePid = whereis(Scope),
    %% do not care about the remote monitor, it is started only to check DOWN handling
    ThirdMonitor = spawn_link(Node, fun() -> second_monitor(ScopePid, Group, Self, SecondMonitor) end),
    Ref3 = receive {ThirdMonitor, ThirdRef} -> ThirdRef end,
    %% remote join
    RemotePid = erlang:spawn(Node, forever()),
    ?assertEqual(ok, rpc:call(Node, spg, join, [Scope, Group, [RemotePid, RemotePid]])),
    wait_message(Ref, join, Group, [RemotePid, RemotePid], "Remote"),
    %% verify leave event
    ?assertEqual([Self], spg:get_local_members(Scope, Group)),
    ?assertEqual(ok, spg:leave(Scope, Group, self())),
    wait_message(Ref, leave, Group, [Self], "Local"),
    %% remote leave
    ?assertEqual(ok, rpc:call(Node, spg, leave, [Scope, Group, RemotePid])),
    %% flush the local pg scope via remote pg (to ensure local pg finished sending notifications)
    sync_via({?FUNCTION_NAME, Node}, ?FUNCTION_NAME),
    wait_message(Ref, leave, Group, [RemotePid], "Remote"),
    %% drop the ExtraMonitor - this keeps original and remote monitors
    SecondMonMsgs = gen_server:call(ExtraMonitor, flush),
    %% inspect the queue, it should contain double remote join, then single local and single remove leave
    ExpectedLocalMessages = [
        {Ref2, join, Group, [RemotePid, RemotePid]},
        {Ref2, leave, Group, [Self]},
        {Ref2, leave, Group, [RemotePid]}],
    ?assertEqual(ExpectedLocalMessages, SecondMonMsgs, "Local monitor failed"),
    %% inspect remote monitor queue
    ThirdMonMsgs = gen_server:call(ThirdMonitor, flush),
    ExpectedRemoteMessages = [
        {Ref3, join, Group, [RemotePid, RemotePid]},
        {Ref3, leave, Group, [Self]},
        {Ref3, leave, Group, [RemotePid]}],
    ?assertEqual(ExpectedRemoteMessages, ThirdMonMsgs, "Remote monitor failed"),
    %% remote leave via stop (causes remote monitor to go DOWN)
    ok = stop_node(Peer, Node),
    wait_message(Ref, leave, Group, [RemotePid], "Remote stop"),
    DownMonitor(Scope, Ref, Self),
    %% demonitor
    ?assertEqual(ok, spg:demonitor(Scope, Ref)),
    ?assertEqual(false, spg:demonitor(Scope, Ref)),
    %% ensure messages don't come
    ?assertEqual(ok, spg:join(Scope, Group, Self)),
    sync(Scope),
    %% join should not be here
    receive {Ref, Action, Group, [Self]} -> ?assert(false, lists:concat(["Unexpected ", Action, "event"]))
    after 0 -> ok end.

wait_message(Ref, Action, Group, Pids, Msg) ->
    receive
        {Ref, Action, Group, Pids} ->
            ok
    after 1000 ->
        {messages, Msgs} = process_info(self(), messages),
        ct:pal("Message queue: ~0p", [Msgs]),
        ?assert(false, lists:flatten(io_lib:format("Expected ~s ~s for ~p", [Msg, Action, Group])))
    end.

second_monitor(Scope, Group, Control, SecondMonitor) ->
    Ref = SecondMonitor(Scope, Group, Control),
    Control ! {self(), Ref},
    second_monitor([]).

second_monitor(Msgs) ->
    receive
        {'$gen_call', Reply, flush} ->
            gen:reply(Reply, lists:reverse(Msgs));
        Msg ->
            second_monitor([Msg | Msgs])
    end.

protocol_upgrade(Config) when is_list(Config) ->
    Scope = ?FUNCTION_NAME,
    Group = ?FUNCTION_NAME,
    {Peer, Node} = spawn_node(Scope),
    PgPid = rpc:call(Node, erlang, whereis, [Scope]),

    RemotePid = erlang:spawn(Node, forever()),
    ok = rpc:call(Node, spg, join, [Scope, Group, RemotePid]),

    %% OTP 26:
    %% Just do a white-box test and verify that pg accepts
    %% a "future" discover message and replies with a sync.
    PgPid ! {discover, self(), "Protocol version (ignore me)"},
    {'$gen_cast', {sync, PgPid, [{Group, [RemotePid]}]}} = receive_any(),

    %% stop the peer
    stop_node(Peer, Node),
    ok.


%%--------------------------------------------------------------------
%% Test Helpers - start/stop additional Erlang nodes

receive_any() ->
    receive M -> M end.

%% flushes GS (GenServer) queue, ensuring that all prior
%%  messages have been processed
sync(GS) ->
    _ = sys:log(GS, get).

%% flushes GS queue from the point of view of a registered process RegName
%%  running on the Node.
sync_via({RegName, Node}, GS) ->
    MyNode = node(),
    rpc:call(Node, sys, replace_state,
        [RegName, fun (S) -> (catch sys:get_state({GS, MyNode})), S end]);

%% flush remote GS queue from local process RegName
sync_via(RegName, {GS, Node}) ->
    sys:replace_state(RegName,
        fun (S) -> _R = (catch sys:get_state({GS, Node})),
            %%io:format("sync_via: ~p -> R = ~p\n", [{GS, Node},_R]),
            S
        end).

ensure_peers_info(Scope, Nodes) ->
    %% Ensures that pg server on local node has gotten info from
    %% pg servers on all Peer nodes passed as argument (assuming
    %% no connection failures).
    %% 
    %% This function assumes that all nodeup messages has been
    %% delivered to all local recipients (pg server) when called.
    %%
    %% Note that this relies on current ERTS implementation; not
    %% language guarantees.
    %%

    sync(Scope),
    %% Known: nodeup handled and discover sent to Peer

    lists:foreach(fun (Node) -> sync({Scope, Node}) end, Nodes),
    %% Known: nodeup handled by Peers and discover sent to local
    %% Known: discover received/handled by Peers and sync sent to local
    %% Known: discover received from Peer
    %% Known: sync received from Peer

    sync(Scope),
    %% Known: discover handled from Peers and sync sent to Peers
    %% Known: sync from Peers handled
    ok.

-ifdef(CURRENTLY_UNUSED_BUT_SERVES_AS_DOC).

ensure_synced(Scope, Nodes) ->
    %% Ensures that the pg server on local node have synced
    %% with pg servers on all Peer nodes (assuming no connection
    %% failures).
    %% 
    %% This function assumes that all nodeup messages has been
    %% delivered to all local recipients (pg server) when called.
    %%
    %% Note that this relies on current ERTS implementation; not
    %% language guarantees.
    %%
    ensure_peer_info(Scope, Node),
    %% Known: local has gotten info from all Peers
    %% Known: discover from Peers handled and sync sent to Peers
    lists:foreach(fun (Node) -> sync({Scope, Node}) end, Nodes),
    %% Known: sync from local handled by Peers
    ok.

-endif.

disconnect_nodes(Nodes) ->
    %% The following is not a language guarantee, but internal
    %% knowledge about current implementation of ERTS and pg.
    %%
    %% The pg server reacts on 'DOWN's via process monitors of
    %% its peers. These are delivered before 'nodedown's from
    %% net_kernel:monitor_nodes(). That is, by waiting for
    %% 'nodedown' from net_kernel:monitor_nodes() we know that
    %% the 'DOWN' has been delivered to the pg server.
    %%
    %% We do this in a separate process to avoid stray
    %% nodeup/nodedown messages in the test process after
    %% the operation...
    F = fun () ->
        ok = net_kernel:monitor_nodes(true),
        lists:foreach(fun (Node) ->
            true = erlang:disconnect_node(Node)
                      end,
            Nodes),
        lists:foreach(fun (Node) ->
            receive {nodedown, Node} -> ok end
                      end,
            Nodes)
        end,
    {Pid, Mon} = spawn_monitor(F),
    receive
        {'DOWN', Mon, process, Pid, Reason} ->
            ?assertEqual(normal, Reason)
    end,
    ok.

%% @doc Kills process Pid and waits for it to exit using monitor,
%%      and yields after (for 1 ms).
-spec stop_proc(pid()) -> ok.
stop_proc(Pid) ->
    monitor(process, Pid),
    erlang:exit(Pid, kill),
    receive
        {'DOWN', _MRef, process, Pid, _Info} ->
            timer:sleep(1)
    end.

forever() ->
    Parent = self(),
    fun() ->
        %% forever() is used both locally and on a remote node,
        %% if used locally, we want to terminate when the
        %% parent terminates in order to not leak process to
        %% later test cases
        Ref = monitor(process,Parent),
        receive
            {'DOWN',Ref,_,_,_} when node() =:= node(Parent) ->
                ok
        end
    end.

%%--------------------------------------------------------------------
%% Test Helpers - start/stop additional Erlang nodes

%% @doc Executes remote call on the node via TCP socket
%%      Used when dist connection is not available, or
%%      when it's undesirable to use one.
-spec rpc(gen_tcp:socket(), module(), atom(), [term()]) -> term().
rpc(Sock, M, F, A) ->
    ok = gen_tcp:send(Sock, term_to_binary({call, M, F, A})),
    inet:setopts(Sock, [{active, once}]),
    receive
        {tcp, Sock, Data} ->
            case binary_to_term(Data) of
                {ok, Val} ->
                    Val;
                {error, Error} ->
                    {badrpc, Error}
            end;
        {tcp_closed, Sock} ->
            error(closed)
    end.

spawn_node(TestCase) ->
    spawn_node(TestCase, TestCase).

-define (LOCALHOST, {127, 0, 0, 1}).

spawn_node(Scope, TestCase) ->
    Self = self(),
    Controller = erlang:spawn(?MODULE, controller, [TestCase, Scope, Self]),
    receive
        {'$node_started', Node, Port} ->
            {ok, Socket} = gen_tcp:connect(?LOCALHOST, Port, [{active, false}, {mode, binary}, {packet, 4}]),
            Controller ! {socket, Socket},
            {Socket, Node};
        Other ->
            error({start_node, TestCase, Other})
    after 60000 ->
        error({start_node, TestCase, timeout})
    end.

spawn_disconnected_node(Scope, TestCase) ->
    {Peer, Node} = spawn_node(Scope, TestCase),
    true = erlang:disconnect_node(Node),
    {Peer, Node}.


%% @private
-spec controller(atom(), atom(), pid()) -> ok.
controller(Name, Scope, Self) ->
    Pa = filename:dirname(code:which(?MODULE)),
    Pa2 = filename:dirname(code:which(spg)),
    Args = lists:concat(["-setcookie ", erlang:get_cookie(),
            " -connect_all false -kernel dist_auto_connect never -noshell -pa ", Pa, " -pa ", Pa2]),
    NodeName = list_to_atom(lists:concat([Name, "-", erlang:unique_integer([positive])])),
    {ok, Node} = test_server:start_node(NodeName, peer, [{args, Args}]),
    case rpc:call(Node, ?MODULE, control, [Scope], 5000) of
        {badrpc, nodedown} ->
            Self ! {badrpc, Node},
            ok;
        {Port, _PgPid} ->
            Self ! {'$node_started', Node, Port},
            controller_wait()
    end.

controller_wait() ->
    Port =
        receive
            {socket, Port0} ->
                Port0
        end,
    MRef = monitor(port, Port),
    receive
        {'DOWN', MRef, port, Port, _Info} ->
            ok
    end.

%% @doc Stops the node previously started with spawn_node,
%%      and also closes the RPC socket.
-spec stop_node(node(), gen_tcp:socket()) -> true.
stop_node(Peer, Node) when Node =/= node() ->
    true = test_server:stop_node(Node),
    Peer =/= undefined andalso gen_tcp:close(Peer),
    ok.

-spec control(Scope :: atom()) -> {Port :: integer(), pid()}.
control(Scope) ->
    Control = self(),
    erlang:spawn(fun () -> server(Control, Scope) end),
    receive
        {port, Port, SpgPid} ->
            {Port, SpgPid};
        Other ->
            error({error, Other})
    end.

server(Control, Scope) ->
    try
        {ok, Pid} = if Scope =:= undefined -> {ok, undefined}; true -> spg:start(Scope) end,
        {ok, Listen} = gen_tcp:listen(0, [{mode, binary}, {packet, 4}, {ip, ?LOCALHOST}]),
        {ok, Port} = inet:port(Listen),
        Control ! {port, Port, Pid},
        {ok, Sock} = gen_tcp:accept(Listen),
        server_loop(Sock)
    catch
        Class:Reason:Stack ->
            Control ! {error, {Class, Reason, Stack}}
    end.

server_loop(Sock) ->
    inet:setopts(Sock, [{active, once}]),
    receive
        {tcp, Sock, Data} ->
            {call, M, F, A} = binary_to_term(Data),
            Ret =
                try
                    erlang:apply(M, F, A) of
                    Res ->
                        {ok, Res}
                catch
                    exit:Reason ->
                        {error, {'EXIT', Reason}};
                    error:Reason ->
                        {error, {'EXIT', Reason}}
                end,
            ok = gen_tcp:send(Sock, term_to_binary(Ret)),
            server_loop(Sock);
        {tcp_closed, Sock} ->
            erlang:halt(1)
    end.
