%%-------------------------------------------------------------------
%% @author Maxim Fedorov <maximfca@gmail.com>
%% Process Groups with eventually consistent membership.
%%
%% Differences (compared to pg2):
%%  * non-existent and empty group treated the same (empty list of pids),
%%     thus create/1 and delete/1 have no effect (and not implemented).
%%     which_groups() return only non-empty groups
%%  * no cluster lock required, and no dependency on global
%%  * all join/leave operations require local process (it's not possible to join
%%     a process from a different node)
%%  * multi-join: join/leave several processes with a single call
%%
%% Why empty groups are not supported:
%%  Unlike a process, group does not have originating node. So it's possible
%% that during net split one node deletes the group, that still exists for
%% another partition. pg2 will recover the group, as soon as net
%% split converges, which is quite unexpected.
%%
%% Exchange protocol:
%%  * when pg process starts, it broadcasts
%%     'discover' message to all nodes in the cluster
%%  * when pg server receives 'discover', it responds with 'sync' message
%%     containing list of groups with all local processes, and starts to
%%     monitor process that sent 'discover' message (assuming it is a part
%%     of an overlay network)
%%  * every pg process monitors 'nodeup' messages to attempt discovery for
%%     nodes that are (re)joining the cluster
%%
%% Leave/join operations:
%%  * processes joining the group are monitored on the local node
%%  * when process exits (without leaving groups prior to exit), local
%%     instance of pg scoped process detects this and sends 'leave' to
%%     all nodes in an overlay network (no remote monitoring done)
%%  * all leave/join operations are serialised through pg server process
%%
-module(spg).
-author("maximfca@gmail.com").


%% API: default scope
-export([
    start_link/0,

    join/2,
    leave/2,
    get_members/1,
    get_local_members/1,
    which_groups/0,
    which_local_groups/0
]).

%% Scoped API: overlay networks
-export([
    start/1,
    start_link/1,

    join/3,
    leave/3,
    get_members/2,
    get_local_members/2,
    which_groups/1,
    which_local_groups/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% Types
-type group() :: any().

-define(DEFAULT_SCOPE, ?MODULE).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server and links it to calling process.
%% Uses default scope, which is the same as as the module name.
-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    start_link(?DEFAULT_SCOPE).

%% @doc
%% Starts the server outside of supervision hierarchy.
-spec start(Scope :: atom()) -> {ok, pid()} | {error, any()}.
start(Scope) when is_atom(Scope) ->
    gen_server:start({local, Scope}, ?MODULE, [Scope], []).

%% @doc
%% Starts the server and links it to calling process.
%% Scope name is supplied.
-spec start_link(Scope :: atom()) -> {ok, pid()} | {error, any()}.
start_link(Scope) when is_atom(Scope) ->
    gen_server:start_link({local, Scope}, ?MODULE, [Scope], []).

%%--------------------------------------------------------------------
%% @doc
%% Joins a single or a list of processes.
%% Group is created automatically.
%% Processes must be local to this node.
-spec join(Group :: group(), PidOrPids :: pid() | [pid()]) -> ok.
join(Group, PidOrPids) ->
    join(?DEFAULT_SCOPE, Group, PidOrPids).

-spec join(Scope :: atom(), Group :: group(), PidOrPids :: pid() | [pid()]) -> ok.
join(Scope, Group, PidOrPids) when is_pid(PidOrPids); is_list(PidOrPids) ->
    ok = ensure_local(PidOrPids),
    gen_server:call(Scope, {join_local, Group, PidOrPids}, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Single or list of processes leaving the group.
%% Processes must be local to this node.
-spec leave(Group :: group(), PidOrPids :: pid() | [pid()]) -> ok.
leave(Group, PidOrPids) ->
    leave(?DEFAULT_SCOPE, Group, PidOrPids).

-spec leave(Scope :: atom(), Group :: group(), PidOrPids :: pid() | [pid()]) -> ok | not_joined.
leave(Scope, Group, PidOrPids) when is_pid(PidOrPids); is_list(PidOrPids) ->
    ok = ensure_local(PidOrPids),
    gen_server:call(Scope, {leave_local, Group, PidOrPids}, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Returns all processes in a group
-spec get_members(Group :: group()) -> [pid()].
get_members(Group) ->
    get_members(?DEFAULT_SCOPE, Group).

-spec get_members(Scope :: atom(), Group :: group()) -> [pid()].
get_members(Scope, Group) ->
    try
        ets:lookup_element(Scope, Group, 2)
    catch
        error:badarg ->
            []
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns processes in a group, running on local node.
-spec get_local_members(Group :: group()) -> [pid()].
get_local_members(Group) ->
    get_local_members(?DEFAULT_SCOPE, Group).

-spec get_local_members(Scope :: atom(), Group :: group()) -> [pid()].
get_local_members(Scope, Group) ->
    try
        ets:lookup_element(Scope, Group, 3)
    catch
        error:badarg ->
            []
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all known groups.
-spec which_groups() -> [Group :: group()].
which_groups() ->
    which_groups(?DEFAULT_SCOPE).

-spec which_groups(Scope :: atom()) -> [Group :: group()].
which_groups(Scope) when is_atom(Scope) ->
    [G || [G] <- ets:match(Scope, {'$1', '_', '_'})].

%%--------------------------------------------------------------------
%% @private
%% Returns a list of groups that have any local processes joined.
-spec which_local_groups() -> [Group :: group()].
which_local_groups() ->
    which_local_groups(?DEFAULT_SCOPE).

-spec which_local_groups(Scope :: atom()) -> [Group :: group()].
which_local_groups(Scope) when is_atom(Scope) ->
    ets:select(Scope, [{{'$1', '_', '$2'}, [{'=/=', '$2', []}], ['$1']}]).

%%--------------------------------------------------------------------
%% Internal implementation

%% gen_server implementation
-record(state, {
    %% ETS table name, and also the registered process name (self())
    scope :: atom(),
    %% monitored local processes and groups they joined
    local = #{} :: #{pid() => {MRef :: reference(), Groups :: [group()]}},
    %% remote node: scope process monitor and map of groups to pids for fast sync routine
    remote = #{} :: #{pid() => {reference(), #{group() => [pid()]}}}
}).

-type state() :: #state{}.

-spec init([Scope :: atom()]) -> {ok, state()}.
init([Scope]) ->
    ok = net_kernel:monitor_nodes(true),
    %% discover all nodes running this scope in the cluster
    broadcast([{Scope, Node} || Node <- nodes()], {discover, self()}),
    Scope = ets:new(Scope, [set, protected, named_table, {read_concurrency, true}]),
    {ok, #state{scope = Scope}}.

-spec handle_call(Call :: {join_local, Group :: group(), Pid :: pid()}
                        | {leave_local, Group :: group(), Pid :: pid()},
                  From :: {pid(), Tag :: any()},
                  State :: state()) -> {reply, ok | not_joined, state()}.

handle_call({join_local, Group, PidOrPids}, _From, #state{scope = Scope, local = Local,
    remote = Remote} = State) ->
    NewMons = join_local(PidOrPids, Group, Local),
    join_local_update_ets(Scope, Group, PidOrPids),
    broadcast(maps:keys(Remote), {join, self(), Group, PidOrPids}),
    {reply, ok, State#state{local = NewMons}};

handle_call({leave_local, Group, PidOrPids}, _From, #state{scope = Scope, local = Local,
    remote = Remote} = State) ->
    case leave_local(PidOrPids, Group, Local) of
        Local ->
            {reply, not_joined, State};
        NewMons ->
            leave_local_update_ets(Scope, Group, PidOrPids),
            broadcast(maps:keys(Remote), {leave, self(), PidOrPids, [Group]}),
            {reply, ok, State#state{local = NewMons}}
    end;

handle_call(_Request, _From, _S) ->
    error(badarg).

-spec handle_cast(
    {sync, Peer :: pid(), Groups :: [{group(), [pid()]}]} |
    {discover, Peer :: pid()} |
    {join, Peer :: pid(), group(), pid() | [pid()]} |
    {leave, Peer :: pid(), pid() | [pid()], [group()]},
    State :: state()) -> {noreply, state()}.

handle_cast({sync, Peer, Groups}, #state{scope = Scope, remote = Remote} = State) ->
    {noreply, State#state{remote = handle_sync(Scope, Peer, Remote, Groups)}};

handle_cast(_, _State) ->
    error(badarg).

-spec handle_info(
    {discover, Peer :: pid()} |
    {join, Peer :: pid(), group(), pid() | [pid()]} |
    {leave, Peer :: pid(), pid() | [pid()], [group()]} |
    {'DOWN', reference(), process, pid(), term()} |
    {nodedown, node()} | {nodeup, node()}, State :: state()) -> {noreply, state()}.

%% remote pid or several pids joining the group
handle_info({join, Peer, Group, PidOrPids}, #state{scope = Scope, remote = Remote} = State) ->
    case maps:get(Peer, Remote, []) of
        {MRef, RemoteGroups} ->
            join_remote_update_ets(Scope, Group, PidOrPids),
            %% store remote group => pids map for fast sync operation
            NewRemoteGroups = join_remote(Group, PidOrPids, RemoteGroups),
            {noreply, State#state{remote = Remote#{Peer => {MRef, NewRemoteGroups}}}};
        [] ->
            %% handle possible race condition, when remote node is flickering up/down,
            %%  and remote join can happen after the node left overlay network
            %% It also handles the case when node outside of overlay network sends
            %%  unexpected join request.
            {noreply, State}
    end;

%% remote pid leaving (multiple groups at once)
handle_info({leave, Peer, PidOrPids, Groups}, #state{scope = Scope, remote = Remote} = State) ->
    case maps:get(Peer, Remote, []) of
        {MRef, RemoteMap} ->
            _ = leave_remote_update_ets(Scope, PidOrPids, Groups),
            NewRemoteMap = leave_remote(PidOrPids, RemoteMap, Groups),
            {noreply, State#state{remote = Remote#{Peer => {MRef, NewRemoteMap}}}};
        [] ->
            %% Handle race condition: remote node disconnected, but scope process
            %%  of the remote node was just about to send 'leave' message. In this
            %%  case, local node handles 'DOWN' first, but then connection is
            %%  restored, and 'leave' message gets delivered when it's not expected.
            %% It also handles the case when node outside of overlay network sends
            %%  unexpected leave request.
            {noreply, State}
    end;

%% we're being discovered, let's exchange!
handle_info({discover, Peer}, #state{remote = Remote, local = Local} = State) ->
    gen_server:cast(Peer, {sync, self(), all_local_pids(Local)}),
    %% do we know who is looking for us?
    case maps:is_key(Peer, Remote) of
        true ->
            {noreply, State};
        false ->
            MRef = erlang:monitor(process, Peer),
            erlang:send(Peer, {discover, self()}, [noconnect]),
            {noreply, State#state{remote = Remote#{Peer => {MRef, #{}}}}}
    end;

%% handle local process exit
handle_info({'DOWN', MRef, process, Pid, _Info}, #state{scope = Scope, local = Local,
    remote = Remote} = State) when node(Pid) =:= node() ->
    case maps:take(Pid, Local) of
        error ->
            %% this can only happen when leave request and 'DOWN' are in pg queue
            {noreply, State};
        {{MRef, Groups}, NewLocal} ->
            [leave_local_update_ets(Scope, Group, Pid) || Group <- Groups],
            %% send update to all remote peers
            broadcast(maps:keys(Remote), {leave, self(), Pid, Groups}),
            {noreply, State#state{local = NewLocal}}
    end;

%% handle remote node down or scope leaving overlay network
handle_info({'DOWN', MRef, process, Pid, _Info}, #state{scope = Scope, remote = Remote} = State)  ->
    {{MRef, RemoteMap}, NewRemote} = maps:take(Pid, Remote),
    maps:foreach(fun (Group, Pids) -> leave_remote_update_ets(Scope, Pids, [Group]) end, RemoteMap),
    {noreply, State#state{remote = NewRemote}};

%% nodedown: ignore, and wait for 'DOWN' signal for monitored process
handle_info({nodedown, _Node}, State) ->
    {noreply, State};

%% nodeup: discover if remote node participates in the overlay network
handle_info({nodeup, Node}, State) when Node =:= node() ->
    {noreply, State};
handle_info({nodeup, Node}, #state{scope = Scope} = State) ->
    {Scope, Node} ! {discover, self()},
    {noreply, State};

handle_info(_Info, _State) ->
    error(badarg).

-spec terminate(Reason :: any(), State :: state()) -> true.
terminate(_Reason, #state{scope = Scope}) ->
    true = ets:delete(Scope).

%%--------------------------------------------------------------------
%% Internal implementation

%% Ensures argument is either a node-local pid or a list of such, or it throws an error
ensure_local(Pid) when is_pid(Pid), node(Pid) =:= node() ->
    ok;
ensure_local(Pids) when is_list(Pids) ->
    lists:foreach(
        fun
            (Pid) when is_pid(Pid), node(Pid) =:= node() ->
                ok;
            (Bad) ->
                error({nolocal, Bad})
        end, Pids);
ensure_local(Bad) ->
    erlang:error({nolocal, Bad}).

%% Override all knowledge of the remote node with information it sends
%%  to local node. Current implementation must do the full table scan
%%  to remove stale pids (just as for 'nodedown').
handle_sync(Scope, Peer, Remote, Groups) ->
    %% can't use maps:get() because it evaluates 'default' value first,
    %%   and in this case monitor() call has side effect.
    {MRef, RemoteGroups} =
        case maps:find(Peer, Remote) of
            error ->
                {erlang:monitor(process, Peer), #{}};
            {ok, MRef0} ->
                MRef0
        end,
    %% sync RemoteMap and transform ETS table
    _ = sync_groups(Scope, RemoteGroups, Groups),
    Remote#{Peer => {MRef, maps:from_list(Groups)}}.

sync_groups(Scope, RemoteGroups, []) ->
    %% leave all missing groups
    [leave_remote_update_ets(Scope, Pids, [Group]) || {Group, Pids} <- maps:to_list(RemoteGroups)];
sync_groups(Scope, RemoteGroups, [{Group, Pids} | Tail]) ->
    case maps:take(Group, RemoteGroups) of
        {Pids, NewRemoteGroups} ->
            sync_groups(Scope, NewRemoteGroups, Tail);
        {OldPids, NewRemoteGroups} ->
            [{Group, AllOldPids, LocalPids}] = ets:lookup(Scope, Group),
            %% should be really rare...
            AllNewPids = Pids ++ AllOldPids -- OldPids,
            true = ets:insert(Scope, {Group, AllNewPids, LocalPids}),
            sync_groups(Scope, NewRemoteGroups, Tail);
        error ->
            join_remote_update_ets(Scope, Group, Pids),
            sync_groups(Scope, RemoteGroups, Tail)
    end.

join_local(Pid, Group, Local) when is_pid(Pid) ->
    case maps:find(Pid, Local) of
        {ok, {MRef, Groups}} ->
            maps:put(Pid, {MRef, [Group | Groups]}, Local);
        error ->
            MRef = erlang:monitor(process, Pid),
            Local#{Pid => {MRef, [Group]}}
    end;
join_local([], _Group, Local) ->
    Local;
join_local([Pid | Tail], Group, Local) ->
    join_local(Tail, Group, join_local(Pid, Group, Local)).

join_local_update_ets(Scope, Group, Pid) when is_pid(Pid) ->
    case ets:lookup(Scope, Group) of
        [{Group, All, Local}] ->
            ets:insert(Scope, {Group, [Pid | All], [Pid | Local]});
        [] ->
            ets:insert(Scope, {Group, [Pid], [Pid]})
    end;
join_local_update_ets(Scope, Group, Pids) ->
    case ets:lookup(Scope, Group) of
        [{Group, All, Local}] ->
            ets:insert(Scope, {Group, Pids ++ All, Pids ++ Local});
        [] ->
            ets:insert(Scope, {Group, Pids, Pids})
    end.

join_remote_update_ets(Scope,  Group, Pid) when is_pid(Pid) ->
    case ets:lookup(Scope, Group) of
        [{Group, All, Local}] ->
            ets:insert(Scope, {Group, [Pid | All], Local});
        [] ->
            ets:insert(Scope, {Group, [Pid], []})
    end;
join_remote_update_ets(Scope, Group, Pids) ->
    case ets:lookup(Scope, Group) of
        [{Group, All, Local}] ->
            ets:insert(Scope, {Group, Pids ++ All, Local});
        [] ->
            ets:insert(Scope, {Group, Pids, []})
    end.

join_remote(Group, Pid, RemoteGroups) when is_pid(Pid) ->
    maps:update_with(Group, fun (List) -> [Pid | List] end, [Pid], RemoteGroups);
join_remote(Group, Pids, RemoteGroups) ->
    maps:update_with(Group, fun (List) -> Pids ++ List end, Pids, RemoteGroups).

leave_local(Pid, Group, Local) when is_pid(Pid) ->
    case maps:find(Pid, Local) of
        {ok, {MRef, [Group]}} ->
            erlang:demonitor(MRef),
            maps:remove(Pid, Local);
        {ok, {MRef, Groups}} ->
            case lists:member(Group, Groups) of
                true ->
                    maps:put(Pid, {MRef, lists:delete(Group, Groups)}, Local);
                false ->
                    Local
            end;
        _ ->
            Local
    end;
leave_local([], _Group, Local) ->
    Local;
leave_local([Pid | Tail], Group, Local) ->
    leave_local(Tail, Group, leave_local(Pid, Group, Local)).

leave_local_update_ets(Scope, Group, Pid) when is_pid(Pid) ->
    case ets:lookup(Scope, Group) of
        [{Group, [Pid], [Pid]}] ->
            ets:delete(Scope, Group);
        [{Group, All, Local}] ->
            ets:insert(Scope, {Group, lists:delete(Pid, All), lists:delete(Pid, Local)});
        [] ->
            %% rare race condition when 'DOWN' from monitor stays in msg queue while process is leave-ing.
            true
    end;
leave_local_update_ets(Scope, Group, Pids) ->
    case ets:lookup(Scope, Group) of
        [{Group, All, Local}] ->
            case All -- Pids of
                [] ->
                    ets:delete(Scope, Group);
                NewAll ->
                    ets:insert(Scope, {Group, NewAll, Local -- Pids})
            end;
        [] ->
            true
    end.

leave_remote_update_ets(Scope, Pid, Groups) when is_pid(Pid) ->
    _ = [
        case ets:lookup(Scope, Group) of
            [{Group, [Pid], []}] ->
                ets:delete(Scope, Group);
            [{Group, All, Local}] ->
                ets:insert(Scope, {Group, lists:delete(Pid, All), Local});
            [] ->
                true
        end ||
        Group <- Groups];
leave_remote_update_ets(Scope, Pids, Groups) ->
    _ = [
        case ets:lookup(Scope, Group) of
            [{Group, All, Local}] ->
                case All -- Pids of
                    [] when Local =:= [] ->
                        ets:delete(Scope, Group);
                    NewAll ->
                        ets:insert(Scope, {Group, NewAll, Local})
                end;
            [] ->
                true
        end ||
        Group <- Groups].

leave_remote(Pid, RemoteMap, Groups) when is_pid(Pid) ->
    leave_remote([Pid], RemoteMap, Groups);
leave_remote(Pids, RemoteMap, Groups) ->
    lists:foldl(
        fun (Group, Acc) ->
            case maps:get(Group, Acc) -- Pids of
                [] ->
                    maps:remove(Group, Acc);
                Remaining ->
                    Acc#{Group => Remaining}
            end
        end, RemoteMap, Groups).

all_local_pids(Local) ->
    maps:to_list(maps:fold(
        fun(Pid, {_Ref, Groups}, Acc) ->
            lists:foldl(
                fun(Group, Acc1) ->
                    Acc1#{Group => [Pid | maps:get(Group, Acc1, [])]}
                end, Acc, Groups)
        end, #{}, Local)).

%% Works as gen_server:abcast(), but accepts a list of processes
%%   instead of nodes list.
broadcast([], _Msg) ->
    ok;
broadcast([Dest | Tail], Msg) ->
    %% do not use 'nosuspend', as it will lead to missing
    %%   join/leave messages when dist buffer is full
    erlang:send(Dest, Msg, [noconnect]),
    broadcast(Tail, Msg).
