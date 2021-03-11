%%-------------------------------------------------------------------
%% @author Maxim Fedorov <maximfca@gmail.com>
%% @doc
%% Scalable Process Groups cluster-wide test, uses PropEr stateful
%%  testing routine.
%% @end
%% -------------------------------------------------------------------
-module(spg_cluster_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    init_per_suite/1, end_per_suite/1,
    all/0
]).

%% Test cases exports
-export([
    spg_sequential/0, spg_sequential/1
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
    stop_proc/2,
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
    spg_SUITE:init_per_suite(Config).

end_per_suite(Config) ->
    spg_SUITE:end_per_suite(Config).

all() ->
    [spg_sequential].

%%--------------------------------------------------------------------
%% PropEr shims implementation

%% A test case runs up to 17 nodes in total, sharing a single scope.
%% Actual amount is determined on test case startup.
-define (MAX_PEER_COUNT, 16).

%% 4 groups - to provoke collisions
-define(GROUPS, [one, two, three, four]).

rpc({Node, _Sock}, _M, _F, _A) when Node =:= node() ->
    error({local, Node});
rpc({_Node, Peer}, M, F, A) ->
    catch peer:apply(Peer, M, F, A).

start_node() ->
    Node = peer:random_name(false),
    {ok, Peer} = peer:start_link(#{node => Node, connection => 0,
        args => ["-connect_all", "false", "-kernel", "dist_auto_connect", "never",
            "-pz", filename:dirname(code:which(spg))], longnames => false}),
    {Node, Peer}.

stop_node({_Node, Peer}) ->
    ok =:= gen_server:stop(Peer).

start_scope(Access, Scope) ->
    rpc(Access, spg, start, [Scope]).

stop_scope(Access, Scope) ->
    rpc(Access, gen_server, stop, [Scope]).

start_proc(Access) ->
    rpc(Access, erlang, spawn, [fun() -> receive after infinity -> ok end end]).

stop_proc(Access, Pid) ->
    rpc(Access, spg_SUITE, stop_proc, [Pid]).

connect_peer(Access, NodeTwo) ->
    rpc(Access, net_kernel, connect_node, [NodeTwo]),
    State = rpc(Access, sys, get_state, [net_kernel]),
    ?assertEqual(state, element(1, State)).

disconnect_peer(Access, NodeTwo) ->
    rpc(Access, erlang, disconnect_node, [NodeTwo]),
    State = rpc(Access, sys, get_state, [net_kernel]),
    ?assertEqual(state, element(1, State)).

join_group(Access, Scope, PidOrPids, Group) ->
    rpc(Access, spg, join, [Scope, Group, PidOrPids]).

leave_group(Access, Scope, PidOrPids, Group) ->
    rpc(Access, spg, leave, [Scope, Group, PidOrPids]).

get_members(Access, Scope, Group) ->
    rpc(Access, spg, get_members, [Scope, Group]).

get_local_members(Access, Scope, Group) ->
    rpc(Access, spg, get_local_members, [Scope, Group]).


%%--------------------------------------------------------------------
%% PropEr tests

%% Single node state (bar name, which is the map key)
-record(node, {
    %% scope up or not?
    scope = false :: boolean(),
    %% node controller process
    socket :: gen_tcp:socket(),
    %% dist connections from this node
    links = [] :: [node()],
    %% local pids of the node, and groups it joined
    procs = #{} :: #{pid() => [any()]}
}).

-record(state, {
    scope_name :: atom(),
    peer_count :: pos_integer(),
    nodes = #{} :: #{node() => #node{}}
}).

%%--------------------------------------------------------------------
%% PropEr state machine

scope_name(Extra) ->
    %% form scope name as OS pid + own pid
    [_, Pid, _] = string:split(io_lib:format("~p", [self()]), ".", all),
    list_to_atom(lists:concat(["scope_", os:getpid(), "_", Pid, "-", Extra])).

initial_state() ->
    #state{scope_name = scope_name(""), peer_count = erlang:phash2(self(), ?MAX_PEER_COUNT) + 2}.

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
precondition(#state{nodes = Nodes, peer_count = Count}, {call, ?MODULE, start_node, []}) ->
    map_size(Nodes) < Count;
precondition(#state{nodes = Nodes}, {call, ?MODULE, stop_node, [{Name, _}]}) ->
    is_map_key(Name, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, start_scope, [{Name, _}, _Scope]}) ->
    is_map_key(Name, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, stop_scope, [{Name, _}, _Scope]}) ->
    is_map_key(Name, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, start_proc, [{Name, _}]}) ->
    is_map_key(Name, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, stop_proc, [{Name, _}, Pid]}) ->
    proc_exist(Name, Nodes, Pid);
precondition(#state{nodes = Nodes}, {call, ?MODULE, connect_peer, [{Name1, _}, Name2]}) ->
    case maps:find(Name1, Nodes) of
        {ok, #node{links = Links}} ->
            lists:member(Name2, Links) =:= false;
        _ ->
            false
    end;
precondition(#state{nodes = Nodes}, {call, ?MODULE, disconnect_peer, [{Name1, _}, Name2]}) ->
    case maps:find(Name1, Nodes) of
        {ok, #node{links = Links}} ->
            lists:member(Name2, Links);
        _ ->
            false
    end;
precondition(#state{nodes = Nodes}, {call, ?MODULE, join_group, [{Name, _}, _Scope, Pids, _Group]}) when is_list(Pids) ->
    procs_exist(Name, Nodes, Pids);
precondition(#state{nodes = Nodes}, {call, ?MODULE, join_group, [{Name, _}, _Scope, Pid, _Group]}) ->
    proc_exist(Name, Nodes, Pid);
precondition(#state{nodes = Nodes}, {call, ?MODULE, leave_group, [{Name, _}, _Scope, Pids, _Group]}) when is_list(Pids) ->
    procs_exist(Name, Nodes, Pids);
precondition(#state{nodes = Nodes}, {call, ?MODULE, leave_group, [{Name, _}, _Scope, Pid, _Group]}) ->
    proc_exist(Name, Nodes, Pid);
precondition(#state{nodes = Nodes}, {call, ?MODULE, get_members, [{Name, _}, _Scope, _Group]}) ->
    is_map_key(Name, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, get_local_members, [{Name, _}, _Scope, _Group]}) ->
    is_map_key(Name, Nodes).

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

postcondition(#state{nodes = Nodes}, {call, ?MODULE, join_group, [{Name, _}, _Scope, _Pid, _Group]}, Res) ->
    test_scope_up(Nodes, Name, Res);

postcondition(#state{nodes = Nodes}, {call, ?MODULE, leave_group, [{Name, _}, _Scope, _Pid, _Group]}, Res) ->
    test_scope_up(Nodes, Name, Res);

postcondition(#state{nodes = Nodes}, {call, ?MODULE, start_scope, [{Name, _}, _Scope]}, Res) ->
    #{Name := #node{scope = ScopeUp}} = Nodes,
    case {ScopeUp, Res} of
        {true, {error, {already_started, Pid}}} when is_pid(Pid) ->
            true;
        {false, {ok, Pid}} when is_pid(Pid) ->
            true;
        _ ->
            false
    end;

postcondition(#state{nodes = Nodes}, {call, ?MODULE, stop_scope, [{Name, _}, _Scope]}, Res) ->
    test_scope_up(Nodes, Name, Res);

postcondition(#state{nodes = Nodes}, {call, ?MODULE, get_local_members, [{Name, _} = Access, Scope, Group]}, Res) ->
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
                    rpc(Access, sys, get_log, [Scope]),
                    Synced = lists:sort(rpc(Access, spg, get_local_members, [Scope, Group])),
                    Synced =:= Local
            end
    end;

postcondition(#state{nodes = Nodes}, {call, ?MODULE, get_members, [{Name, _} = Node0, Scope, Group]}, Res) ->
    %% collect this group from all connected nodes
    #{Name := #node{links = Links, scope = ScopeUp}} = Nodes,
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
                end, [], [Name | Links])),
            SortedRes = lists:sort(Res),
            %%
            SortedRes == Global orelse retry_get_members([Name | Links], Node0, Scope, Group, Global, 20)
    end;

postcondition(_State, _Cmd, _Res) ->
    true.

retry_get_members(Nodes, Node, Scope, Group, Expected, 0) ->
    Res = lists:sort(get_members(Node, Scope, Group)),
    Missing = Expected -- Res, Extra = Res -- Expected,
    Res =/= Expected andalso
        io:fwrite(user, "get_members failed: ~nMissing: ~p~nExtra: ~p~nNodes: ~p~nExpected nodes: ~p~n",
            [Missing, Extra, rpc(Node, erlang, nodes, []), Nodes]),
    false;
retry_get_members(Nodes, Node, Scope, Group, Expected, Left) ->
    timer:sleep(5),
    Res = lists:sort(get_members(Node, Scope, Group)),
    Res == Expected orelse retry_get_members(Nodes, Node, Scope, Group, Expected, Left - 1).

%% generates commands to bring link up or down
links({Self, _} = Access, Nodes, Existing) ->
    [
        {call, ?MODULE,
            case lists:member(N, Existing) of
                true ->
                    disconnect_peer;
                false ->
                    connect_peer
            end,
            [Access, N]} || N <- Nodes, N =/= Self
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
command(#state{scope_name = Scope, nodes = Nodes0, peer_count = PeerCount}) ->
    %% there could be a chance to start one more node
    StartNode = [{call, ?MODULE, start_node, []} || map_size(Nodes0) < PeerCount],

    %% generate all possible commands for all nodes:
    Nodes = maps:keys(Nodes0),
    Commands = StartNode ++ maps:fold(
        fun (Node, #node{links = Links, procs = Procs, socket = Socket}, Cmds) ->
                Access = {Node, Socket},
                [
                    {call, ?MODULE, stop_node, [Access]}
                ] ++
                    basic(Access, Scope) ++ links(Access, Nodes, Links) ++
                        procs(Access, Scope, maps:keys(Procs)) ++ Cmds
        end, [], Nodes0),
    oneof(Commands).

next_state(#state{nodes = Nodes} = State, Res, {call, ?MODULE, start_node, []}) ->
    Name = {call,erlang,element,[1, Res]},
    %% start with no links
    Nodes2 = Nodes#{Name => #node{socket = {call,erlang,element,[2, Res]}, links = []}},
    State#state{nodes = Nodes2};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, stop_node, [{Name, _}]}) ->
    %% remove links from all nodes
    Nodes1 = maps:map(
        fun (_N, #node{links = Links} = Node) ->
            Node#node{links = lists:delete(Name, Links)}
        end, Nodes),
    State#state{nodes = maps:remove(Name, Nodes1)};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, start_scope, [{Name, _}, _Scope]}) ->
    #{Name := Node} = Nodes,
    State#state{nodes = Nodes#{Name => Node#node{scope = true}}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, stop_scope, [{Name, _}, _Scope]}) ->
    #{Name := Node} = Nodes,
    %% all node local procs lose group membership on this node, and on other nodes too
    %% However processes stay alive (and can re-join)
    RunningProcs = maps:map(fun (_, _) -> [] end, Node#node.procs),
    State#state{nodes = Nodes#{Name => Node#node{scope = false, procs = RunningProcs}}};

next_state(#state{nodes = Nodes} = State, Res, {call, ?MODULE, start_proc, [{Name, _}]}) ->
    #{Name := Node} = Nodes,
    State#state{nodes = Nodes#{Name => Node#node{procs = maps:put(Res, [], Node#node.procs)}}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, stop_proc, [{Name, _}, Pid]}) ->
    #{Name := Node} = Nodes,
    State#state{nodes = Nodes#{Name => Node#node{procs = maps:remove(Pid, Node#node.procs)}}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, connect_peer, [{Name1, _}, Name2]}) ->
    #{Name1 := Node1} = Nodes,
    #{Name2 := Node2} = Nodes,
    State#state{nodes = Nodes#{
        Name1 => Node1#node{links = [Name2 | Node1#node.links]},
        Name2 => Node2#node{links = [Name1 | Node2#node.links]}
    }};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, disconnect_peer, [{Name1, _}, Name2]}) ->
    #{Name1 := Node1} = Nodes,
    #{Name2 := Node2} = Nodes,
    State#state{nodes = Nodes#{
        Name1 => Node1#node{links = lists:delete(Name2, Node1#node.links)},
        Name2 => Node2#node{links = lists:delete(Name1, Node2#node.links)}
    }};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, join_group, [{Name, _}, _Scope, Pids, Group]}) when is_list(Pids) ->
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

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, join_group, [{Name, _}, _Scope, Pid, Group]}) ->
    #{Name := #node{scope = ScopeUp} = Node} = Nodes,
    if ScopeUp ->
        State#state{nodes = Nodes#{Name => Node#node{
            procs = maps:update_with(Pid, fun (L) -> [Group | L] end, Node#node.procs)}}};
        true ->
            State
    end;

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, leave_group, [{Name, _}, _Scope, Pids, Group]}) when is_list(Pids) ->
    %% may ignore scope up/down
    #{Name := Node} = Nodes,
    Node1 = lists:foldl(
        fun (Pid, Node0) ->
            Node0#node{procs = maps:update_with(Pid, fun (L) -> lists:delete(Group, L) end, Node0#node.procs)}
        end, Node, Pids),
    State#state{nodes = Nodes#{Name => Node1}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, leave_group, [{Name, _}, _Scope, Pid, Group]}) ->
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
                [(catch gen_node:stop(Sock)) || #node{socket = Sock} <- maps:values(State#state.nodes)],
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

spg_sequential() ->
    [{doc, "Sequential quickcheck/PropEr test, several copies executed concurrently"}, {timetrap, {hours, 2}}].

spg_sequential(Config) when is_list(Config) ->
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
