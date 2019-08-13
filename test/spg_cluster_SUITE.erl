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
    all/0
]).

%% Test cases exports
-export([
    spg_proper_check/0, spg_proper_check/1
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
    start_node/1,
    stop_node/1,
    start_scope/2,
    stop_scope/2,
    start_proc/1,
    stop_proc/2,
    connect_peer/2,
    disconnect_peer/2,
    join_group/3,
    leave_group/3,
    get_members/2,
    get_local_members/2
]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [spg_proper_check].

%%--------------------------------------------------------------------
%% PropEr shims (wrapping around spg)

%% 4 nodes in total (including self-node)
-define(PEER_NODES, [node1, node2, node3]).

%% 4 groups - to provoke collisions
-define(GROUPS, [one, two, three, four]).


-define (PROPER_SERVER, proper).

start_node(Name0) ->
    Name = case string:lexemes(atom_to_list(Name0), "@") of
               [Name1, _] ->
                   Name1;
               [Name2] ->
                   Name2
           end,
    {Name0, Socket} = spgt:spawn_node(?PROPER_SERVER, Name),
    Socket.

stop_node({_, Node, Socket}) ->
    true = spgt:stop_node(Node, Socket).

start_scope(Access, Scope) ->
    impl(Access, spgt, start_scope, [Scope]).

stop_scope(Access, Scope) ->
    impl(Access, spgt, stop_scope, [Scope]).

% spawn: do not use 'impl', as 'spawn' does not need rpc
start_proc({direct, _, _}) ->
    spgt:spawn();
start_proc({true, Node, _}) ->
    spgt:spawn(Node);
start_proc({false, _, Socket}) ->
    spgt:rpc(Socket, spgt, spawn, []).

stop_proc(Access, Pid) ->
    impl(Access, spgt, stop_proc, [Pid]).

connect_peer(Access, NodeTwo) ->
    %true = connect_peer(Access, To, connect_peer_impl(Access, NodeTwo), 5).

%connect_peer(_Access, _To, true, _) ->
%    true;
%connect_peer(Access, To, false, 1) ->
%    connect_peer_impl(Access, To);
%connect_peer(Access, To, false, Retry) ->
%    connect_peer(Access, To, connect_peer_impl(Access, To), Retry - 1).

%connect_peer_impl(Access, NodeTwo) ->
    impl(Access, net_kernel, connect_node, [NodeTwo]).

disconnect_peer(Access, NodeTwo) ->
    impl(Access, erlang, disconnect_node, [NodeTwo]).

join_group(Access, PidOrPids, Group) ->
    impl(Access, spg, join, [?PROPER_SERVER, Group, PidOrPids]).

leave_group(Access, PidOrPids, Group) ->
    impl(Access, spg, leave, [?PROPER_SERVER, Group, PidOrPids]).

get_members(Access, Group) ->
    impl(Access, spg, get_members, [?PROPER_SERVER, Group]).

get_local_members(Access, Group) ->
    impl(Access, spg, get_local_members, [?PROPER_SERVER, Group]).

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
    case spgt:rpc(Socket, M, F, A) of
        {badrpc, Reason} ->
            Reason;
        Other ->
            Other
    end.


%%--------------------------------------------------------------------
%% PropEr tests

%% Single node state (bar name, which is the map key)
-record(node, {
    up = false :: boolean(),
    % scope up or not? by default it is
    scope = true :: boolean(),
    % controlling (rpc) socket
    socket :: undefined | gen_tcp:socket(),
    % dist connections from this node
    links = [] :: [node()],
    % local pids of the node, and groups it joined
    procs = #{} :: #{pid() => [any()]}
}).

%%--------------------------------------------------------------------
%% PropEr state machine

initial_state() ->
    maps:from_list([{node(), #node{up = true}} |
        %% generate full node name same way test_server does, it breaks layering,
        %%  but compatible with PropEr way to store variables
        [{list_to_atom(lists:concat([Node, "@", test_server_sup:hoststr()])), #node{}} ||
            Node <- ?PEER_NODES]]).

precondition(_State, _Cmd) ->
    true.

test_scope_up(State, Name, Res) ->
    #{Name := #node{scope = ScopeUp}} = State,
    case {ScopeUp, Res} of
        {true, ok} ->
            true;
        {false, {'EXIT', noproc}} ->
            true;
        {false, {'EXIT', {noproc, _}}} ->
            true;
        _ ->
            erlang:display({"Error", Name, Res, ScopeUp}),
            false
    end.

postcondition(State, {call, ?MODULE, join_group, [{_, Name, _}, _Pid, _Group]}, Res) ->
    test_scope_up(State, Name, Res);

postcondition(State, {call, ?MODULE, leave_group, [{_, Name, _}, _Pid, _Group]}, Res) ->
    test_scope_up(State, Name, Res);

postcondition(State, {call, ?MODULE, start_scope, [{_, Name, _}, _Scope]}, Res) ->
    #{Name := #node{scope = ScopeUp}} = State,
    case {ScopeUp, Res} of
        {true, {error, {already_started, Pid}}} when is_pid(Pid) ->
            true;
        {false, {ok, Pid}} when is_pid(Pid) ->
            true;
        _ ->
            false
    end;

postcondition(State, {call, ?MODULE, stop_scope, [{_, Name, _}, _Scope]}, Res) ->
    test_scope_up(State, Name, Res);

postcondition(State, {call, ?MODULE, get_local_members, [{_, Name, _}, Group]}, Res) ->
    #{Name := #node{procs = Procs, scope = ScopeUp}} = State,
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

postcondition(State, {call, ?MODULE, get_members, [{_, Name, _} = Node0, Group]}, Res) ->
    % collect this group from all connected nodes
    #{Name := #node{links = Links, scope = ScopeUp}} = State,
    case ScopeUp of
        false ->
            Res =:= [];
        true ->
            Global = lists:sort(lists:foldl(
                fun (N, Acc) ->
                    #{N := #node{procs = Procs}} = State,
                    maps:fold(
                        fun (Pid, Groups, Acc1) ->
                            [Pid || G <- Groups, G =:= Group] ++ Acc1
                        end,
                        Acc, Procs)
                end, [], [Name | Links])),
            SortedRes = lists:sort(Res),
            SortedRes == Global orelse retry_get_members(Node0, Group, Global, 10)
    end;

postcondition(_State, _Cmd, _Res) ->
    true.

retry_get_members(_Node, _Group, _Expected, 0) ->
    false;
retry_get_members(Node, Group, Expected, Left) ->
    timer:sleep(5),
    Res = lists:sort(get_members(Node, Group)),
    % erlang:display({"Global RETRY: Have: ", Res, "Expect: ", Expected, " in group ", Group}),
    Res == Expected orelse retry_get_members(Node, Group, Expected, Left - 1).

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
procs(_Access, []) ->
    [];
procs(Access, Procs) ->
    [
        {call, ?MODULE, stop_proc, [Access, elements(Procs)]},
        {call, ?MODULE, join_group, [Access, elements(Procs), elements(?GROUPS)]},
        {call, ?MODULE, leave_group, [Access, elements(Procs), elements(?GROUPS)]},
        {call, ?MODULE, join_group, [Access, list(oneof(Procs)), elements(?GROUPS)]},
        {call, ?MODULE, leave_group, [Access, list(oneof(Procs)), elements(?GROUPS)]}
    ].

access(Node, _) when Node =:= node() ->
    {direct, Node, undefined};
access(Node, #node{socket = Socket, links = Links}) ->
    {lists:member(node(), Links), Node, Socket}.

basic(Access) ->
    [
        {call, ?MODULE, start_proc, [Access]},
        {call, ?MODULE, start_scope, [Access, ?PROPER_SERVER]},
        {call, ?MODULE, stop_scope, [Access, ?PROPER_SERVER]},
        {call, ?MODULE, get_local_members, [Access, elements(?GROUPS)]},
        {call, ?MODULE, get_members, [Access, elements(?GROUPS)]}
    ].

%% original idea was to throw in a list of nodes X list of ops, but... it won't work,
%   because we'll sockets bindings, proc ids etc.
command(State) ->
    % need to have a list of nodes up (for links management)
    NodesUp = [Node || {Node, #node{up = true}} <- maps:to_list(State)],
    %% generate all possible commands for all nodes:
    Commands = maps:fold(
        fun (Node, #node{up = false}, Cmds) ->
                [{call, ?MODULE, start_node, [Node]} | Cmds];
            (Node, #node{links = Links, procs = Procs} = Info, Cmds) ->
                Access = access(Node, Info),
                [
                    {call, ?MODULE, stop_node, [Access]} || node() =/= Node
                ] ++
                    basic(Access) ++ links(Access, NodesUp, Links) ++
                        procs(Access, maps:keys(Procs)) ++ Cmds
        end, [], State),
    oneof(Commands).

next_state(State, Socket, {call, ?MODULE, start_node, [Name]}) ->
    #{Name := Node} = State,
    % also change master node state
    State1 = maps:update_with(node(),
        fun (#node{links = Links} = Master) ->
            Master#node{links = [Name | Links]}
        end, State),
    State1#{Name => Node#node{up = true, socket = Socket, links = [node()]}};
next_state(State, _Res, {call, ?MODULE, stop_node, [{_, Name, _}]}) ->
    % remove links from all nodes
    State1 = maps:map(
        fun (_N, #node{links = Links} = Node) ->
            Node#node{links = lists:delete(Name, Links)}
        end, State),
    State1#{Name => #node{}};

next_state(State, _Res, {call, ?MODULE, start_scope, [{_, Name, _}, _Scope]}) ->
    #{Name := Node} = State,
    State#{Name => Node#node{scope = true}};
next_state(State, _Res, {call, ?MODULE, stop_scope, [{_, Name, _}, _Scope]}) ->
    #{Name := Node} = State,
    State#{Name => Node#node{scope = false, procs = #{}}};

next_state(State, Res, {call, ?MODULE, start_proc, [{_, Name, _}]}) ->
    #{Name := Node} = State,
    State#{Name => Node#node{procs = maps:put(Res, [], Node#node.procs)}};
next_state(State, _Res, {call, ?MODULE, stop_proc, [{_, Name, _}, Pid]}) ->
    #{Name := Node} = State,
    State#{Name => Node#node{procs = maps:remove(Pid, Node#node.procs)}};

next_state(State, _Res, {call, ?MODULE, connect_peer, [{_, Name1, _}, Name2]}) ->
    #{Name1 := Node1} = State,
    #{Name2 := Node2} = State,
    State#{
        Name1 => Node1#node{links = [Name2 | Node1#node.links]},
        Name2 => Node2#node{links = [Name1 | Node2#node.links]}
    };
next_state(State, _Res, {call, ?MODULE, disconnect_peer, [{_, Name1, _}, Name2]}) ->
    #{Name1 := Node1} = State,
    #{Name2 := Node2} = State,
    State#{
        Name1 => Node1#node{links = lists:delete(Name2, Node1#node.links)},
        Name2 => Node2#node{links = lists:delete(Name1, Node2#node.links)}
    };

next_state(State, _Res, {call, ?MODULE, join_group, [{_, Name, _}, Pids, Group]}) when is_list(Pids) ->
    #{Name := #node{scope = ScopeUp} = Node} = State,
    Node1 =
        if ScopeUp ->
            lists:foldl(
                fun (Pid, Node0) ->
                    Node0#node{procs = maps:update_with(Pid, fun (L) -> [Group | L] end, Node0#node.procs)}
                end, Node, Pids);
            true ->
                Node
        end,
    State#{Name => Node1};

next_state(State, _Res, {call, ?MODULE, join_group, [{_, Name, _}, Pid, Group]}) ->
    #{Name := #node{scope = ScopeUp} = Node} = State,
    if ScopeUp ->
        State#{Name => Node#node{
            procs = maps:update_with(Pid, fun (L) -> [Group | L] end, Node#node.procs)}};
        true ->
            State
    end;

next_state(State, _Res, {call, ?MODULE, leave_group, [{_, Name, _}, Pids, Group]}) when is_list(Pids) ->
    % may ignore scope up/down
    #{Name := Node} = State,
    Node1 = lists:foldl(
        fun (Pid, Node0) ->
            Node0#node{procs = maps:update_with(Pid, fun (L) -> lists:delete(Group, L) end, Node0#node.procs)}
        end, Node, Pids),
    State#{Name => Node1};

next_state(State, _Res, {call, ?MODULE, leave_group, [{_, Name, _}, Pid, Group]}) ->
    % may ignore scope up/down
    #{Name := Node} = State,
    State#{Name => Node#node{procs = maps:update_with(Pid, fun (L) -> lists:delete(Group, L) end, Node#node.procs)}};

next_state(State, _Res, _Call) ->
    State.

prop_spg_no_crash(Config) when is_list(Config) ->
    ?FORALL(Cmds, commands(?MODULE),
        ?TRAPEXIT(
            begin
                {ok, _Pid} = spg:start_link(?PROPER_SERVER),
                {History, State, Result} = proper_statem:run_commands(?MODULE, Cmds),
                % cleanup: kill slave nodes
                Name = node(),
                % kill processes
                #{Name := #node{procs = Procs}} = State,
                [exit(P, kill) || P <- maps:keys(Procs)],
                % stop spg server
                whereis(?PROPER_SERVER) =/= undefined andalso gen_server:stop(?PROPER_SERVER),
                % close sockets & stop controlling procs
                [spgt:stop_node(N, Sock) ||
                    {N, #node{up = true, socket = Sock}} <- maps:to_list(State), Name =/= N],
                test_server_ctrl:kill_slavenodes(),
                % check no slaves are still running
                [] = ets:match_object(slave_tab,'_'),
                rand:uniform(80) =:= 80 andalso io:fwrite(user, "\n", []),
                ?WHENFAIL(
                    begin
                        erlang:display(Result),
                        erlang:display(case History of [] -> "init"; _ -> lists:nth(length(History), Cmds) end),
                        ct:pal("Failed: ~200p~nCommands: ~200p~nHistory: ~200p~nState: ~200p~nResult: ~200p",
                            [case History of [] -> "init"; _ -> lists:nth(length(History), Cmds) end,
                                Cmds, History, State, Result])
                    end,
                    Result =:= ok)
            end)).

spg_proper_check() ->
    [{doc, "PropEr tests for spg module, long, 2 hours timeout"}, {timetrap, {seconds, 240 * 60}}].

spg_proper_check(Config) ->
    proper:quickcheck(prop_spg_no_crash(Config), [{numtests, 20000}, {to_file, user}]).
