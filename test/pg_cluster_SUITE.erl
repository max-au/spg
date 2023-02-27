%%-------------------------------------------------------------------
%% @author Maxim Fedorov <maximfca@gmail.com>
%% Property-based test for `pg' module in Erlang/OTP.
-module(pg_cluster_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    init_per_suite/1, end_per_suite/1,
    all/0
]).

%% Test cases exports
-export([
    pg_sequential/0, pg_sequential/1
]).

-behaviour(proper_statem).

%% PropEr state machine
-export([
    initial_state/0,
    command/1,
    precondition/2,
    next_state/3,
    postcondition/3
]).

%% PropEr shims
-export([
    start_node/0,
    stop_node/1,
    start_scope/2,
    stop_scope/2,
    start_proc/1,
    stop_proc/1, stop_proc/2,
    connect_peer/2,
    disconnect_peer/2,
    join_group/4,
    leave_group/4,
    get_members/3,
    get_local_members/3
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("proper/include/proper.hrl").

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

all() ->
    [pg_sequential].

%%--------------------------------------------------------------------
%% PropEr shims implementation

%% A test case runs up to 17 nodes in total, sharing a single scope.
-define (MAX_PEER_COUNT, 16).

%% 4 groups - to provoke collisions
-define(GROUPS, [one, two, three, four]).

start_node() ->
    {ok, Pid, Node} = peer:start_link(#{name => peer:random_name(?MODULE), connection => standard_io,
        args => ["-connect_all", "false", "-kernel", "dist_auto_connect", "never",
            "-pz", filename:dirname(code:which(pg)), "-pz", filename:dirname(code:which(?MODULE))]}),
    register(Node, Pid),
    Node.

stop_node(Peer) ->
    ok =:= peer:stop(Peer).

start_scope(Peer, Scope) ->
    catch peer:call(Peer, pg, start, [Scope]).

stop_scope(Peer, Scope) ->
    catch peer:call(Peer, gen_server, stop, [Scope]).

start_proc(Peer) ->
    catch peer:call(Peer, erlang, spawn, [fun() -> receive after infinity -> ok end end]).

stop_proc(Peer, Pid) ->
    catch peer:call(Peer, ?MODULE, stop_proc, [Pid]).

connect_peer(Peer, NodeTwo) ->
    peer:call(Peer, net_kernel, connect_node, [NodeTwo], 15000),
    State = peer:call(Peer, sys, get_state, [net_kernel]),
    ?assertEqual(state, element(1, State)).

disconnect_peer(Peer, NodeTwo) ->
    catch peer:call(Peer, erlang, disconnect_node, [NodeTwo]),
    State = peer:call(Peer, sys, get_state, [net_kernel]),
    ?assertEqual(state, element(1, State)).

join_group(Peer, Scope, PidOrPids, Group) ->
    catch peer:call(Peer, pg, join, [Scope, Group, PidOrPids]).

leave_group(Peer, Scope, PidOrPids, Group) ->
    catch peer:call(Peer, pg, leave, [Scope, Group, PidOrPids]).

get_members(Peer, Scope, Group) ->
    catch peer:call(Peer, pg, get_members, [Scope, Group]).

get_local_members(Peer, Scope, Group) ->
    catch peer:call(Peer, pg, get_local_members, [Scope, Group]).

stop_proc(Pid) ->
    monitor(process, Pid),
    erlang:exit(Pid, kill),
    receive
        {'DOWN', _MRef, process, Pid, _Info} ->
            timer:sleep(1)
    end.

%%--------------------------------------------------------------------
%% PropEr tests

%% Single node state (bar name, which is the map key)
-record(node, {
    %% scope up or not?
    scope = false :: boolean(),
    %% dist connections from this node
    links = [] :: [node()],
    %% local pids of the node, and groups it joined
    procs = #{} :: #{pid() => [any()]}
}).

-record(state, {
    scope_name :: atom(),
    nodes = #{} :: #{node() => #node{}}
}).

%%--------------------------------------------------------------------
%% PropEr state machine

scope_name(Extra) ->
    %% form scope name as OS pid + own pid
    [_, Pid, _] = string:split(io_lib:format("~p", [self()]), ".", all),
    list_to_atom(lists:concat(["scope_", os:getpid(), "_", Pid, "-", Extra])).

initial_state() ->
    #state{scope_name = scope_name("init")}.

proc_exist(Name, Nodes, Pid) ->
    case maps:find(Name, Nodes) of
        {ok, #node{procs = Procs}} ->
            is_map_key(Pid, Procs);
        _ ->
            false
    end.

procs_exist(Name, Nodes, Pids) ->
    case maps:find(Name, Nodes) of
        {ok, #node{procs = Procs}} ->
            lists:all(fun (Pid) -> is_map_key(Pid, Procs) end, Pids);
        _ ->
            false
    end.

%% repeat command generation logic.
%% Yes, it is highly desirable to use something like hypothesis based testing,
%%   but so far there is no convenient tool to do that, hence, let's just repeat the logic.
precondition(#state{nodes = Nodes}, {call, ?MODULE, start_node, []}) ->
    map_size(Nodes) < ?MAX_PEER_COUNT;
precondition(#state{nodes = Nodes}, {call, ?MODULE, stop_node, [Node]}) ->
    is_map_key(Node, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, start_scope, [Node, _Scope]}) ->
    is_map_key(Node, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, stop_scope, [Node, _Scope]}) ->
    is_map_key(Node, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, start_proc, [Node]}) ->
    is_map_key(Node, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, stop_proc, [Node, Pid]}) ->
    proc_exist(Node, Nodes, Pid);
precondition(#state{nodes = Nodes}, {call, ?MODULE, connect_peer, [Node, Name2]}) ->
    case maps:find(Node, Nodes) of
        {ok, #node{links = Links}} when Node =/= Name2 ->
            lists:member(Name2, Links) =:= false;
        _ ->
            false
    end;
precondition(#state{nodes = Nodes}, {call, ?MODULE, disconnect_peer, [Node, Name2]}) ->
    case maps:find(Node, Nodes) of
        {ok, #node{links = Links}} when Node =/= Name2 ->
            lists:member(Name2, Links);
        _ ->
            false
    end;
precondition(#state{nodes = Nodes}, {call, ?MODULE, join_group, [Node, _Scope, Pids, _Group]}) when is_list(Pids) ->
    procs_exist(Node, Nodes, Pids);
precondition(#state{nodes = Nodes}, {call, ?MODULE, join_group, [Node, _Scope, Pid, _Group]}) ->
    proc_exist(Node, Nodes, Pid);
precondition(#state{nodes = Nodes}, {call, ?MODULE, leave_group, [Node, _Scope, Pids, _Group]}) when is_list(Pids) ->
    procs_exist(Node, Nodes, Pids);
precondition(#state{nodes = Nodes}, {call, ?MODULE, leave_group, [Node, _Scope, Pid, _Group]}) ->
    proc_exist(Node, Nodes, Pid);
precondition(#state{nodes = Nodes}, {call, ?MODULE, get_members, [Node, _Scope, _Group]}) ->
    is_map_key(Node, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, get_local_members, [Node, _Scope, _Group]}) ->
    is_map_key(Node, Nodes).

test_scope_up(Nodes, Name, Res) ->
    #{Name := #node{scope = ScopeUp}} = Nodes,
    case {ScopeUp, Res} of
        {true, ok} ->
            true;
        {true, not_joined} ->
            %% TODO: implement not_joined state for leave_group call
            true;
        {false, {badrpc, {'EXIT', noproc}}} ->
            true;
        {false, {badrpc, {'EXIT', {noproc, _}}}} ->
            true;
        {false, {'EXIT', noproc}} ->
            true;
        {false, {'EXIT', {noproc, _}}} ->
            true;
        _ ->
            io:fwrite(user, "Unexpected: scope up: ~s with result = ~p on ~p ~n", [ScopeUp, Res, Name]),
            false
    end.

postcondition(#state{nodes = Nodes}, {call, ?MODULE, join_group, [Name, _Scope, _Pid, _Group]}, Res) ->
    test_scope_up(Nodes, Name, Res);

postcondition(#state{nodes = Nodes}, {call, ?MODULE, leave_group, [Name, _Scope, _Pid, _Group]}, Res) ->
    test_scope_up(Nodes, Name, Res);

postcondition(#state{nodes = Nodes}, {call, ?MODULE, start_scope, [Name, _Scope]}, Res) ->
    #{Name := #node{scope = ScopeUp}} = Nodes,
    case {ScopeUp, Res} of
        {true, {error, {already_started, Pid}}} when is_pid(Pid) ->
            true;
        {false, {ok, Pid}} when is_pid(Pid) ->
            true;
        _ ->
            false
    end;

postcondition(#state{nodes = Nodes}, {call, ?MODULE, stop_scope, [Name, _Scope]}, Res) ->
    test_scope_up(Nodes, Name, Res);

postcondition(#state{nodes = Nodes}, {call, ?MODULE, get_local_members, [Name, Scope, Group]}, Res) ->
    #{Name := #node{procs = Procs, scope = ScopeUp}} = Nodes,
    case ScopeUp of
        false ->
            Res =:= [];
        true ->
            Local = lists:sort(maps:fold(
                fun (Pid, Groups, Acc) ->
                    [Pid || G <- Groups, G =:= Group] ++ Acc
                end, [], Procs)),
            %% optimistic case
            case lists:sort(Res) of
                Local ->
                    true;
                _Actual ->
                    %% optimistic didn't work, go sync and try again
                    ProcState = peer:call(Name, sys, get_state, [Scope]),
                    Synced = lists:sort(peer:call(Name, pg, get_local_members, [Scope, Group])),
                    Synced =/= Local andalso
                        io:fwrite(user, "Incorrect local members on node ~s (scope ~s)~nExpected ~200p~nActual ~200p~nProcState: ~200p~n",
                            [Name, Scope, Local, Synced, ProcState]),
                    Synced =:= Local
            end
    end;

postcondition(#state{nodes = Nodes}, {call, ?MODULE, get_members, [Node, Scope, Group]}, Res) ->
    %% collect this group from all connected nodes
    #{Node := #node{links = Links, scope = ScopeUp}} = Nodes,
    case ScopeUp of
        false ->
            Res =:= [];
        true ->
            Global = lists:sort(lists:foldl(
                fun (N, Acc) ->
                    #node{procs = Procs, scope = IsRemoteUp} = maps:get(N, Nodes),
                    case IsRemoteUp of
                        true ->
                            maps:fold(
                                fun (Pid, Groups, Acc1) ->
                                    [Pid || G <- Groups, G =:= Group] ++ Acc1
                                end,
                                Acc, Procs);
                        false ->
                            Acc
                    end
                end, [], [Node | Links])),
            SortedRes = lists:sort(Res),
            %%
            SortedRes == Global orelse retry_get_members([Node | Links], Node, Scope, Group, Global, 20)
    end;

postcondition(_State, _Cmd, _Res) ->
    true.

retry_get_members(Nodes, Node, Scope, Group, Expected, 0) ->
    Res = lists:sort(get_members(Node, Scope, Group)),
    Missing = Expected -- Res, Extra = Res -- Expected,
    Res =/= Expected andalso
        io:fwrite(user, "get_members on ~s (scope ~s) failed: ~nMissing: ~p~nExtra: ~p~nNodes: ~p~nExpected nodes: ~p~n",
            [Node, Scope, Missing, Extra, peer:call(Node, erlang, nodes, []), Nodes -- [Node]]),
    false;
retry_get_members(Nodes, Node, Scope, Group, Expected, Left) ->
    timer:sleep(5),
    Res = lists:sort(get_members(Node, Scope, Group)),
    Res == Expected orelse retry_get_members(Nodes, Node, Scope, Group, Expected, Left - 1).

%% generates commands to bring link up or down
links(Node, Nodes, Existing) ->
    [
        {call, ?MODULE,
            case lists:member(N, Existing) of
                true ->
                    disconnect_peer;
                false ->
                    connect_peer
            end,
            [Node, N]} || N <- Nodes, N =/= Node
    ].

%% generates commands for procs: stop, leave/join
procs(_Access, _Scope, []) ->
    [];
procs(Access, Scope, Procs) ->
    [
        {call, ?MODULE, stop_proc, [Access, elements(Procs)]},
        {call, ?MODULE, join_group, [Access, Scope, elements(Procs), elements(?GROUPS)]},
        {call, ?MODULE, leave_group, [Access, Scope, elements(Procs), elements(?GROUPS)]},
        {call, ?MODULE, join_group, [Access, Scope, list(oneof(Procs)), elements(?GROUPS)]},
        {call, ?MODULE, leave_group, [Access, Scope, list(oneof(Procs)), elements(?GROUPS)]}
    ].

basic(Access, Scope) ->
    [
        {call, ?MODULE, start_proc, [Access]},
        {call, ?MODULE, start_scope, [Access, Scope]},
        {call, ?MODULE, stop_scope, [Access, Scope]},
        {call, ?MODULE, get_local_members, [Access, Scope, elements(?GROUPS)]},
        {call, ?MODULE, get_members, [Access, Scope, elements(?GROUPS)]}
    ].

%% original idea was to throw in a list of nodes X list of ops, but... it won't work,
%%  because of different sockets bindings, proc ids etc.
command(#state{scope_name = Scope, nodes = Nodes0}) ->
    %% there could be a chance to start one more node
    StartNode = [{call, ?MODULE, start_node, []} || map_size(Nodes0) < ?MAX_PEER_COUNT],

    %% generate all possible commands for all nodes:
    Nodes = maps:keys(Nodes0),
    Commands = StartNode ++ maps:fold(
        fun (Node, #node{links = Links, procs = Procs}, Cmds) ->
                [
                    {call, ?MODULE, stop_node, [Node]}
                ] ++
                    basic(Node, Scope) ++ links(Node, Nodes, Links) ++
                        procs(Node, Scope, maps:keys(Procs)) ++ Cmds
        end, [], Nodes0),
    oneof(Commands).

next_state(#state{nodes = Nodes} = State, Res, {call, ?MODULE, start_node, []}) ->
    %% start with no links
    Nodes2 = Nodes#{Res => #node{links = []}},
    State#state{nodes = Nodes2};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, stop_node, [Name]}) ->
    %% remove links from all nodes
    Nodes1 = maps:map(
        fun (_N, #node{links = Links} = Node) ->
            Node#node{links = lists:delete(Name, Links)}
        end, Nodes),
    State#state{nodes = maps:remove(Name, Nodes1)};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, start_scope, [Name, _Scope]}) ->
    #{Name := Node} = Nodes,
    State#state{nodes = Nodes#{Name => Node#node{scope = true}}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, stop_scope, [Name, _Scope]}) ->
    #{Name := Node} = Nodes,
    %% all node local procs lose group membership on this node, and on other nodes too
    %% However processes stay alive (and can re-join)
    RunningProcs = maps:map(fun (_, _) -> [] end, Node#node.procs),
    State#state{nodes = Nodes#{Name => Node#node{scope = false, procs = RunningProcs}}};

next_state(#state{nodes = Nodes} = State, Res, {call, ?MODULE, start_proc, [Name]}) ->
    #{Name := Node} = Nodes,
    State#state{nodes = Nodes#{Name => Node#node{procs = maps:put(Res, [], Node#node.procs)}}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, stop_proc, [Name, Pid]}) ->
    #{Name := Node} = Nodes,
    State#state{nodes = Nodes#{Name => Node#node{procs = maps:remove(Pid, Node#node.procs)}}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, connect_peer, [Name1, Name2]}) ->
    #{Name1 := Node1} = Nodes,
    #{Name2 := Node2} = Nodes,
    State#state{nodes = Nodes#{
        Name1 => Node1#node{links = [Name2 | Node1#node.links]},
        Name2 => Node2#node{links = [Name1 | Node2#node.links]}
    }};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, disconnect_peer, [Name1, Name2]}) ->
    #{Name1 := Node1} = Nodes,
    #{Name2 := Node2} = Nodes,
    State#state{nodes = Nodes#{
        Name1 => Node1#node{links = lists:delete(Name2, Node1#node.links)},
        Name2 => Node2#node{links = lists:delete(Name1, Node2#node.links)}
    }};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, join_group, [Name, _Scope, Pids, Group]}) when is_list(Pids) ->
    #{Name := #node{scope = ScopeUp} = Node} = Nodes,
    Node1 =
        if ScopeUp ->
            lists:foldl(
                fun (Pid, Node0) ->
                    Node0#node{procs = maps:update_with(Pid, fun (L) -> [Group | L] end, Node0#node.procs)}
                end, Node, Pids);
            true ->
                Node
        end,
    State#state{nodes = Nodes#{Name => Node1}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, join_group, [Name, _Scope, Pid, Group]}) ->
    #{Name := #node{scope = ScopeUp} = Node} = Nodes,
    if ScopeUp ->
        State#state{nodes = Nodes#{Name => Node#node{
            procs = maps:update_with(Pid, fun (L) -> [Group | L] end, Node#node.procs)}}};
        true ->
            State
    end;

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, leave_group, [Name, _Scope, Pids, Group]}) when is_list(Pids) ->
    %% may ignore scope up/down
    #{Name := Node} = Nodes,
    Node1 = lists:foldl(
        fun (Pid, Node0) ->
            Node0#node{procs = maps:update_with(Pid, fun (L) -> lists:delete(Group, L) end, Node0#node.procs)}
        end, Node, Pids),
    State#state{nodes = Nodes#{Name => Node1}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, leave_group, [Name, _Scope, Pid, Group]}) ->
    %% may ignore scope up/down
    #{Name := Node} = Nodes,
    State#state{nodes = Nodes#{Name =>
        Node#node{procs = maps:update_with(Pid, fun (L) -> lists:delete(Group, L) end, Node#node.procs)}}};

next_state(State, _Res, {call, ?MODULE, get_members, [_Access, _Scope, _Group]}) ->
    State;

next_state(State, _Res, {call, ?MODULE, get_local_members, [_Access, _Scope, _Group]}) ->
    State.

%%--------------------------------------------------------------------
%% PropEr properties

prop_sequential(Control) ->
    ?FORALL(Cmds, proper_statem:commands(?MODULE),
        begin
            try
                {History, State, Result} = proper_statem:run_commands(?MODULE, Cmds),
                %% cleanup: kill slave nodes
                [peer:stop(N) || N <- maps:keys(State#state.nodes)],
                %% report progress
                Control ! progress,
                Result =/= ok andalso (Control ! {error, {Result, State, History, Cmds}}),
                Result =:= ok
            catch
                Class:Reason:Stack ->
                    Control ! {error, {Class, Reason, Stack}}
            end
        end
    ).

%%--------------------------------------------------------------------
%% Actual test cases

pg_sequential() ->
    [{doc, "Sequential quickcheck/PropEr test, several copies executed concurrently"}, {timetrap, {hours, 2}}].

pg_sequential(Config) when is_list(Config) ->
    TotalTests = 10000,
    Control = self(),
    %% Run as many threads as there are schedulers, multiplied by 2 for some better concurrency
    Online = erlang:system_info(schedulers_online) * 2,
    %%  ... and every model does part of the job
    TestsPerProcess = TotalTests div Online,
    ?assert(TestsPerProcess > 5), %% otherwise this test makes no sense
    %% First process received additional items
    Extra = TotalTests - (TestsPerProcess * Online),
    TestProcs = [TestsPerProcess + Extra | [TestsPerProcess || _ <- lists:seq(2, Online)]],
    Workers =
        [
            proc_lib:spawn_link(
                fun () ->
                    Result = proper:quickcheck(prop_sequential(Control), [{numtests, NumTests},
                        {max_size, 500}, {start_size, 30}]),
                    Control ! {self(), Result}
                end
            ) || NumTests <- TestProcs
        ],
    wait_result(erlang:system_time(second), 0, TotalTests, Workers).

wait_result(_Started, _Done, _Total, []) ->
    ok;
wait_result(Started, Done, Total, Workers) ->
    receive
        progress ->
            Done rem 50 == 49 andalso
                begin
                    Done1 = Done + 1,
                    Now = erlang:system_time(second),
                    Passed = Now - Started,
                    Speed = Done1 / Passed,
                    Remaining = erlang:round((Total - Done1) / Speed),
                    io:fwrite(user, "~b of ~b complete (elapsed ~s, ~s left, ~.2f per second) ~n",
                        [Done1, Total, format_time(Passed), format_time(Remaining), Speed])
                end,
            wait_result(Started, Done + 1, Total, Workers);
        {Pid, true} ->
            wait_result(Started, Done, Total, lists:delete(Pid, Workers));
        {error, {Result, State, History, Cmds}} ->
            Failed = if History =:= [] -> hd(Cmds); true -> lists:nth(length(History), Cmds) end,
            io:fwrite(user,
                "=======~nCommand: ~120p~n~nState: ~120p~n~n=======~nResult: ~120p~n~nCommands: ~120p~n~nHistory: ~200p~n~n",
                [Failed, State, Result, Cmds, History]),
            {fail, {error, Result}};
        {error, Reason} ->
            {fail, {error, Reason}};
        CounterExample ->
            {fail, {commands, CounterExample}}
    end.

format_time(Timer) when Timer > 3600 ->
    io_lib:format("~2..0b:~2..0b:~2..0b", [Timer div 3600, Timer rem 3600 div 60, Timer rem 60]);
format_time(Timer) when Timer > 60 ->
    io_lib:format("~2..0b:~2..0b", [Timer div 60, Timer rem 60]);
format_time(Timer) ->
    io_lib:format("~2b sec", [Timer]).
