%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%%     Scalable Process Groups cluster-wide test, uses PropEr stateful
%%% testing routine.
%%% @end
%%% -------------------------------------------------------------------
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

% PropEr shims
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

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

init_per_suite(Config) ->
    spg_SUITE:init_per_suite(Config).

end_per_suite(Config) ->
    spg_SUITE:end_per_suite(Config).

all() ->
    [spg_sequential].

%%--------------------------------------------------------------------
%% PropEr shims (wrapping around spg)

%% 4 nodes in total (excluding self)
-define (PEER_COUNT, 3).

%% 4 groups - to provoke collisions
-define(GROUPS, [one, two, three, four]).

forever() ->
    fun() -> receive after infinity -> ok end end.

start_node() ->
    % generate node name based on os pid & self pid + time
    Name = scope_name(integer_to_list(erlang:system_time(microsecond))),
    {ok, Peer} = local_node:start_link(Name, #{auto_connect => true,
        connection => {undefined, undefined},
        connect_all => false, code_path => [filename:dirname(code:which(spg))]}),
    Node = gen_node:get_node(Peer),
    {Node, Peer}.

stop_node({_, Node, Socket}) when Node =/= node() ->
    gen_node:stop(Socket),
    true.

start_scope(Access, Scope) ->
    impl(Access, gen_server, start, [{local, Scope}, spg, [Scope], []]).

stop_scope(Access, Scope) ->
    impl(Access, gen_server, stop, [Scope]).

% spawn: do not use 'impl', as 'spawn' does not need rpc
start_proc({direct, _, _}) ->
    spawn(forever());
start_proc({true, Node, _}) ->
    erlang:spawn(Node, forever());
start_proc({false, _, Socket}) ->
    gen_node:rpc(Socket, erlang, spawn, [forever()]).

stop_proc(Access, Pid) ->
    impl(Access, spg_SUITE, stop_proc, [Pid]).

connect_peer(Access, NodeTwo) ->
    impl(Access, net_kernel, connect_node, [NodeTwo]).

disconnect_peer(Access, NodeTwo) ->
    impl(Access, erlang, disconnect_node, [NodeTwo]).

join_group(Access, Scope, PidOrPids, Group) ->
    impl(Access, spg, join, [Scope, Group, PidOrPids]).

leave_group(Access, Scope, PidOrPids, Group) ->
    impl(Access, spg, leave, [Scope, Group, PidOrPids]).

get_members(Access, Scope, Group) ->
    impl(Access, spg, get_members, [Scope, Group]).

get_local_members(Access, Scope, Group) ->
    impl(Access, spg, get_local_members, [Scope, Group]).

-define (RPC_TIMEOUT, 5000).

impl({direct, _, _}, M, F, A) ->
    catch erlang:apply(M, F, A);
impl({true, Node, _}, M, F, A) ->
    case rpc:call(Node, M, F, A, ?RPC_TIMEOUT) of
        {badrpc, Reason} ->
            Reason;
        Other ->
            Other
    end;
impl({false, _, Socket}, M, F, A) ->
    catch gen_node:rpc(Socket, M, F, A).


%%--------------------------------------------------------------------
%% PropEr tests

%% Single node state (bar name, which is the map key)
-record(node, {
    % scope up or not?
    scope = false :: boolean(),
    % controlling (rpc) socket
    socket :: undefined | gen_tcp:socket(),
    % dist connections from this node
    links = [] :: [node()],
    % local pids of the node, and groups it joined
    procs = #{} :: #{pid() => [any()]}
}).

-record(state, {
    scope_name :: atom(),
    nodes :: #{node() => #node{}}
}).

%%--------------------------------------------------------------------
%% PropEr state machine

scope_name(Extra) ->
    % form scope name as OS pid + own pid
    [_, Pid, _] = string:split(io_lib:format("~p", [self()]), ".", all),
    % result should be like 'proper_12345_123'
    list_to_atom(lists:flatten(io_lib:format("scope_~s_~s_~s", [os:getpid(), Pid, Extra]))).

initial_state() ->
    #state{scope_name = scope_name(""), nodes = #{node() => #node{}}}.

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

% repeat command generation logic.
% Yes, it is highly desirable to use something like hypothesis based testing,
%   but so far there is no convenient tool to do that, hence, let's just repeat the logic.
precondition(#state{nodes = Nodes}, {call, ?MODULE, start_node, []}) ->
    map_size(Nodes) < ?PEER_COUNT;
precondition(#state{nodes = Nodes}, {call, ?MODULE, stop_node, [{_, Name, _}]}) ->
    Name =/= node() andalso is_map_key(Name, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, start_scope, [{_, Name, _}, _Scope]}) ->
    is_map_key(Name, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, stop_scope, [{_, Name, _}, _Scope]}) ->
    is_map_key(Name, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, start_proc, [{_, Name, _}]}) ->
    is_map_key(Name, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, stop_proc, [{_, Name, _}, Pid]}) ->
    proc_exist(Name, Nodes, Pid);
precondition(#state{nodes = Nodes}, {call, ?MODULE, connect_peer, [{_, Name1, _}, Name2]}) ->
    case maps:find(Name1, Nodes) of
        {ok, #node{links = Links}} ->
            lists:member(Name2, Links) =:= false;
        _ ->
            false
    end;
precondition(#state{nodes = Nodes}, {call, ?MODULE, disconnect_peer, [{_, Name1, _}, Name2]}) ->
    case maps:find(Name1, Nodes) of
        {ok, #node{links = Links}} ->
            lists:member(Name2, Links);
        _ ->
            false
    end;
precondition(#state{nodes = Nodes}, {call, ?MODULE, join_group, [{_, Name, _}, _Scope, Pids, _Group]}) when is_list(Pids) ->
    procs_exist(Name, Nodes, Pids);
precondition(#state{nodes = Nodes}, {call, ?MODULE, join_group, [{_, Name, _}, _Scope, Pid, _Group]}) ->
    proc_exist(Name, Nodes, Pid);
precondition(#state{nodes = Nodes}, {call, ?MODULE, leave_group, [{_, Name, _}, _Scope, Pids, _Group]}) when is_list(Pids) ->
    procs_exist(Name, Nodes, Pids);
precondition(#state{nodes = Nodes}, {call, ?MODULE, leave_group, [{_, Name, _}, _Scope, Pid, _Group]}) ->
    proc_exist(Name, Nodes, Pid);
precondition(#state{nodes = Nodes}, {call, ?MODULE, get_members, [{_, Name, _}, _Scope, _Group]}) ->
    is_map_key(Name, Nodes);
precondition(#state{nodes = Nodes}, {call, ?MODULE, get_local_members, [{_, Name, _}, _Scope, _Group]}) ->
    is_map_key(Name, Nodes).

test_scope_up(Nodes, Name, Res) ->
    #{Name := #node{scope = ScopeUp}} = Nodes,
    case {ScopeUp, Res} of
        {true, ok} ->
            true;
        {true, not_joined} ->
            %% TODO: implement not_joined state for leave_group call
            true;
        {false, {'EXIT', noproc}} ->
            true;
        {false, {'EXIT', {noproc, _}}} ->
            true;
        {false, {noproc, _}} ->
            true;
        _ ->
            false
    end.

postcondition(#state{nodes = Nodes}, {call, ?MODULE, join_group, [{_, Name, _}, _Scope, _Pid, _Group]}, Res) ->
    test_scope_up(Nodes, Name, Res);

postcondition(#state{nodes = Nodes}, {call, ?MODULE, leave_group, [{_, Name, _}, _Scope, _Pid, _Group]}, Res) ->
    test_scope_up(Nodes, Name, Res);

postcondition(#state{nodes = Nodes}, {call, ?MODULE, start_scope, [{_, Name, _}, _Scope]}, Res) ->
    #{Name := #node{scope = ScopeUp}} = Nodes,
    case {ScopeUp, Res} of
        {true, {error, {already_started, Pid}}} when is_pid(Pid) ->
            true;
        {false, {ok, Pid}} when is_pid(Pid) ->
            true;
        _ ->
            false
    end;

postcondition(#state{nodes = Nodes}, {call, ?MODULE, stop_scope, [{_, Name, _}, _Scope]}, Res) ->
    test_scope_up(Nodes, Name, Res);

postcondition(#state{nodes = Nodes}, {call, ?MODULE, get_local_members, [{_, Name, _}, _Scope, Group]}, Res) ->
    #{Name := #node{procs = Procs, scope = ScopeUp}} = Nodes,
    case ScopeUp of
        false ->
            Res =:= [];
        true ->
            Local = maps:fold(
                fun (Pid, Groups, Acc) ->
                    [Pid || G <- Groups, G =:= Group] ++ Acc
                end, [], Procs),
            lists:sort(Res) =:= lists:sort(Local)
    end;

postcondition(#state{nodes = Nodes}, {call, ?MODULE, get_members, [{_, Name, _} = Node0, Scope, Group]}, Res) ->
    % collect this group from all connected nodes
    #{Name := #node{links = Links, scope = ScopeUp}} = Nodes,
    case ScopeUp of
        false ->
            Res =:= [];
        true ->
            Global = lists:sort(lists:foldl(
                fun (N, Acc) ->
                    #{N := #node{procs = Procs}} = Nodes,
                    maps:fold(
                        fun (Pid, Groups, Acc1) ->
                            [Pid || G <- Groups, G =:= Group] ++ Acc1
                        end,
                        Acc, Procs)
                end, [], [Name | Links])),
            SortedRes = lists:sort(Res),
            SortedRes == Global orelse retry_get_members(Node0, Scope, Group, Global, 10)
    end;

postcondition(_State, _Cmd, _Res) ->
    true.

retry_get_members(_Node, _Scope, _Group, _Expected, 0) ->
    false;
retry_get_members(Node, Scope, Group, Expected, Left) ->
    timer:sleep(5),
    Res = lists:sort(get_members(Node, Scope, Group)),
    % erlang:display({"Global RETRY: Have: ", Res, "Expect: ", Expected, " in group ", Group}),
    Res == Expected orelse retry_get_members(Node, Scope, Group, Expected, Left - 1).

%% generates commands to bring link up or down
links({_, Self, _} = Access, Nodes, Existing) ->
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

access(Node, _) when Node =:= node() ->
    {direct, Node, undefined};
access(Node, #node{socket = Socket, links = Links}) ->
    {lists:member(node(), Links), Node, Socket}.

basic(Access, Scope) ->
    [
        {call, ?MODULE, start_proc, [Access]},
        {call, ?MODULE, start_scope, [Access, Scope]},
        {call, ?MODULE, stop_scope, [Access, Scope]},
        {call, ?MODULE, get_local_members, [Access, Scope, elements(?GROUPS)]},
        {call, ?MODULE, get_members, [Access, Scope, elements(?GROUPS)]}
    ].

%% original idea was to throw in a list of nodes X list of ops, but... it won't work,
%   because of different sockets bindings, proc ids etc.
command(#state{scope_name = Scope, nodes = Nodes0}) ->
    % there could be a chance to start one more node
    StartNode = [{call, ?MODULE, start_node, []} || map_size(Nodes0) < ?PEER_COUNT],

    %% generate all possible commands for all nodes:
    Nodes = maps:keys(Nodes0),
    Commands = StartNode ++ maps:fold(
        fun (Node, #node{links = Links, procs = Procs} = Info, Cmds) ->
                Access = access(Node, Info),
                [
                    {call, ?MODULE, stop_node, [Access]} || node() =/= Node
                ] ++
                    basic(Access, Scope) ++ links(Access, Nodes, Links) ++
                        procs(Access, Scope, maps:keys(Procs)) ++ Cmds
        end, [], Nodes0),
    oneof(Commands).

next_state(#state{nodes = Nodes} = State, Res, {call, ?MODULE, start_node, []}) ->
    Name = {call,erlang,element,[1, Res]},
    % also change master node state
    Nodes1 = maps:update_with(node(),
        fun (#node{links = Links} = Master) ->
            Master#node{links = [Name | Links]}
        end, Nodes),
    Nodes2 = Nodes1#{Name => #node{socket = {call,erlang,element,[2, Res]}, links = [node()]}},
    State#state{nodes = Nodes2};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, stop_node, [{_, Name, _}]}) ->
    % remove links from all nodes
    Nodes1 = maps:map(
        fun (_N, #node{links = Links} = Node) ->
            Node#node{links = lists:delete(Name, Links)}
        end, Nodes),
    State#state{nodes = maps:remove(Name, Nodes1)};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, start_scope, [{_, Name, _}, _Scope]}) ->
    #{Name := Node} = Nodes,
    State#state{nodes = Nodes#{Name => Node#node{scope = true}}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, stop_scope, [{_, Name, _}, _Scope]}) ->
    #{Name := Node} = Nodes,
    % all node local procs lose group membership on this node, and on other nodes too
    % However processes stay alive (and can re-join)
    RunningProcs = maps:map(fun (_, _) -> [] end, Node#node.procs),
    State#state{nodes = Nodes#{Name => Node#node{scope = false, procs = RunningProcs}}};

next_state(#state{nodes = Nodes} = State, Res, {call, ?MODULE, start_proc, [{_, Name, _}]}) ->
    #{Name := Node} = Nodes,
    State#state{nodes = Nodes#{Name => Node#node{procs = maps:put(Res, [], Node#node.procs)}}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, stop_proc, [{_, Name, _}, Pid]}) ->
    #{Name := Node} = Nodes,
    State#state{nodes = Nodes#{Name => Node#node{procs = maps:remove(Pid, Node#node.procs)}}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, connect_peer, [{_, Name1, _}, Name2]}) ->
    #{Name1 := Node1} = Nodes,
    #{Name2 := Node2} = Nodes,
    State#state{nodes = Nodes#{
        Name1 => Node1#node{links = [Name2 | Node1#node.links]},
        Name2 => Node2#node{links = [Name1 | Node2#node.links]}
    }};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, disconnect_peer, [{_, Name1, _}, Name2]}) ->
    #{Name1 := Node1} = Nodes,
    #{Name2 := Node2} = Nodes,
    State#state{nodes = Nodes#{
        Name1 => Node1#node{links = lists:delete(Name2, Node1#node.links)},
        Name2 => Node2#node{links = lists:delete(Name1, Node2#node.links)}
    }};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, join_group, [{_, Name, _}, _Scope, Pids, Group]}) when is_list(Pids) ->
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

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, join_group, [{_, Name, _}, _Scope, Pid, Group]}) ->
    #{Name := #node{scope = ScopeUp} = Node} = Nodes,
    if ScopeUp ->
        State#state{nodes = Nodes#{Name => Node#node{
            procs = maps:update_with(Pid, fun (L) -> [Group | L] end, Node#node.procs)}}};
        true ->
            State
    end;

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, leave_group, [{_, Name, _}, _Scope, Pids, Group]}) when is_list(Pids) ->
    % may ignore scope up/down
    #{Name := Node} = Nodes,
    Node1 = lists:foldl(
        fun (Pid, Node0) ->
            Node0#node{procs = maps:update_with(Pid, fun (L) -> lists:delete(Group, L) end, Node0#node.procs)}
        end, Node, Pids),
    State#state{nodes = Nodes#{Name => Node1}};

next_state(#state{nodes = Nodes} = State, _Res, {call, ?MODULE, leave_group, [{_, Name, _}, _Scope, Pid, Group]}) ->
    % may ignore scope up/down
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
            {_History, State, Result} = proper_statem:run_commands(?MODULE, Cmds),
            % cleanup: kill slave nodes
            Name = node(),
            % kill processes
            #state{scope_name = ScopeName, nodes = Nodes} = State,
            #{Name := #node{procs = Procs}} = Nodes,
            [exit(P, kill) || P <- maps:keys(Procs)],
            % stop spg server
            whereis(ScopeName) =/= undefined andalso gen_server:stop(ScopeName),
            % close sockets & stop controlling procs
            [(catch gen_node:stop(Sock)) ||
                {N, #node{socket = Sock}} <- maps:to_list(Nodes), Name =/= N],
            % report progress
            Control ! progress,
            Result =:= ok
        end
    ).

%%--------------------------------------------------------------------
%% Actual test cases

spg_sequential() ->
    [{doc, "Sequential quickcheck/PropEr test, several copies executed concurrently"}, {timetrap, {hours, 2}}].

spg_sequential(Config) when is_list(Config) ->
    TotalTests = 10000,
    Control = self(),
    % Run as many threads as there are schedulers, multiplied by 2 for some better concurrency
    Online = erlang:system_info(schedulers_online) * 2,
    %  ... and every model does part of the job
    TestsPerProcess = TotalTests div Online,
    ?assert(TestsPerProcess > 5), % otherwise this test makes no sense
    % First process received additional items
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
                    %erlang:display({Done1, Passed, Speed, Total, Remaining}),
                    io:fwrite(user, "~b of ~b complete (elapsed ~s, ~s left, ~.2f per second) ~n",
                        [Done1, Total, format_time(Passed), format_time(Remaining), Speed])
                end,
            wait_result(Started, Done + 1, Total, Workers);
        {Pid, true} ->
            wait_result(Started, Done, Total, lists:delete(Pid, Workers));
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
