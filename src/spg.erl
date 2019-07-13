%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%% Yet another pg2 reincarnation, built to serve as a (not yet drop-in)
%%%  replacement, omitting some properties of the original implementation.
%%% Differences (compared to pg2):
%%%  * non-existent and empty group treated the same (empty list of pids)
%%%     thus create/1 and delete/1 has no effect (and not implemented),
%%%     and which_groups() return only non-empty groups
%%%  * no global lock is taken before making a change to group (relies on
%%%     operations serialisation through a single process)
%%%  * all join/leave operations require local process (it's not possible to join
%%%     a process from a different node)
%%%
%%% Reasoning for dropping empty groups:
%%%  Unlike a process, group does not have originating node. So it's possible
%%% that during net split one node deletes the group, that still exists for
%%% another partition. Erlang/OTP will recover the group, as soon as net
%%% split converges, which is quite unexpected.
%%%  It is possible to introduce group origin (node that is the owner of
%%% the group), but production examples do not seem to support the necessity
%%% of this approach.
%%% @end
-module(spg).
-author("maximfca@gmail.com").

%% API: pg2 replacement
-export([
    start_link/0,

    join/2,
    leave/2,
    get_members/1,
    get_local_members/1,
    which_groups/0
]).

%% API: scoped version for improved concurrency
-export([
    start_link/1,

    join/3,
    leave/3,
    get_members/2,
    get_local_members/2,
    which_groups/1
]).

%%% gen_server exports
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% Group name can be any type.
%% However for performance reasons an atom would be the best choice.
%% Otherwise ETS will be slower to lookup.
-type name() :: any().

%% TODO: Enable 'pg2' compatibility with changing the default name to pg2
-define(DEFAULT_NAME, ?MODULE).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server and links it to calling process.
%% Used default scope, which is the same as as the module name.
-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    start_link(?DEFAULT_NAME).

%% @doc
%% Starts the server and links it to calling process.
%% Scope name is passed as a parameter.
-spec start_link(Scope :: atom()) -> {ok, pid()} | {error, any()}.
start_link(Scope) when is_atom(Scope) ->
    gen_server:start_link({local, Scope}, ?MODULE, [Scope], []).

%%--------------------------------------------------------------------
%% @doc
%% Joins a single process
%% Group is created automatically.
%% Process must be local to this node.
-spec join(Name :: name(), Pid :: pid() | [pid()]) -> ok.
join(Name, Pid) ->
    join(?DEFAULT_NAME, Name, Pid).

-spec join(Scope :: atom(), Name :: name(), Pid :: pid() | [pid()]) -> ok | already_joined.
join(Scope, Name, Pid) when is_pid(Pid), node(Pid) =:= node() ->
    gen_server:call(Scope, {join_local, Name, Pid}).

%%--------------------------------------------------------------------
%% @doc
%% Single process leaving the group.
%% Process must be local to this node.
-spec leave(Name :: name(), Pid :: pid() | [pid()]) -> ok.
leave(Name, Pid) ->
    leave(?DEFAULT_NAME, Name, Pid).

-spec leave(Scope :: atom(), Name :: name(), Pid :: pid() | [pid()]) -> ok.
leave(Scope, Name, Pid) when node(Pid) =:= node() ->
    gen_server:call(Scope, {leave_local, Name, Pid}).

-spec get_members(Name :: name()) -> [pid()].
get_members(Name) ->
    get_members(?DEFAULT_NAME, Name).

-spec get_members(Scope :: atom(), Name :: name()) -> [pid()].
get_members(Scope, Name) ->
    try
        ets:lookup_element(Scope, Name, 2)
    catch
        error:badarg ->
            []
    end.

-spec get_local_members(Name :: name()) -> [pid()].
get_local_members(Name) ->
    get_local_members(?DEFAULT_NAME, Name).

-spec get_local_members(Scope :: atom(), Name :: name()) -> [pid()].
get_local_members(Scope, Name) ->
    try
        ets:lookup_element(Scope, Name, 3)
    catch
        error:badarg ->
            []
    end.

-spec which_groups() -> [Name :: name()].
which_groups() ->
    which_groups(?DEFAULT_NAME).

-spec which_groups(Scope :: atom()) -> [Name :: name()].
which_groups(Scope) when is_atom(Scope) ->
    [G || [G] <- ets:match(Scope, {'$1', '_', '_'})].

%%--------------------------------------------------------------------
%% Internal implementation

%%% gen_server implementation
-record(state, {
    %% ETS table name, and also the registered process name (self())
    scope :: atom(),
    %% Map of monitored local processes
    monitors = #{} :: #{pid() => {MRef :: reference(), Groups :: [name()]}},
    %% remote nodes mappings, contains maps of pids to groups
    remote_pids = #{} :: #{pid() => [name()]}
    %% local processes that want to receive a notifications for group
    %%  membership changes
    % local_monitors = [] :: [pid()]
}).

-type state() :: #state{}.

-spec init([Scope :: atom()]) -> {ok, state()}.
init([Scope]) ->
    ok = net_kernel:monitor_nodes(true),
    %% use 'new_pg2' and 'nodeup' atoms to mimic pg2 behaviour
    _ = [{Scope, N} ! {new_pg2, node()} || N <- nodes()],  % ask other node for 'exchange'
    Scope = ets:new(Scope, [set, protected, named_table, {read_concurrency, true}]),
    {ok, #state{scope = Scope}}.

-spec handle_call(Call :: {join_local, Name :: name(), Pid :: pid()}
                        | {leave_local, Name :: name(), Pid :: pid()}
                        | {join, Name :: name(), Pid :: pid()}          % pg2 compatibility - remote join
                        | {leave, Name :: name(), Pid :: pid()},        % pg2 compatibility - remote leave
                  From :: {pid(),Tag :: any()},
                  State :: state()) -> {reply, ok, state()}.

handle_call({join_local, Group, Pid}, _From, #state{scope = Scope, monitors = Monitors} = State) ->
    case maps:find(Pid, Monitors) of
        {ok, {MRef, Groups}} ->
            join_local_group(Scope, Group, Pid),
            broadcast(Scope, {join, Group, Pid}),
            {reply, ok, State#state{monitors = maps:put(Pid, {MRef, [Group | Groups]}, Monitors)}};
        error ->
            MRef = erlang:monitor(process, Pid),
            join_local_group(Scope, Group, Pid),
            NewMons = Monitors#{Pid => {MRef, [Group]}},
            % must abcast from this server, otherwise there is race condition
            %   between join and leave done concurrently on this node
            broadcast(Scope, {join, Group, Pid}),
            {reply, ok, State#state{monitors = NewMons}}
    end;

handle_call({leave_local, Group, Pid}, _From, #state{scope = Scope, monitors = Monitors} = State) ->
    case maps:find(Pid, Monitors) of
        {ok, {MRef, [Group]}} ->
            erlang:demonitor(MRef),
            leave_local_group(Scope, Group, Pid),
            broadcast(Scope, {leave, Group, Pid}),
            {reply, ok, State#state{monitors = maps:remove(Pid, Monitors)}};
        {ok, {MRef, Groups}} ->
            case lists:member(Group, Groups) of
                true ->
                    leave_local_group(Scope, Group, Pid),
                    broadcast(Scope, {leave, Group, Pid}),
                    {reply, ok, State#state{monitors = maps:put(Pid, {MRef, lists:delete(Group, Groups)}, Monitors)}};
                false ->
                    {reply, ok, State}
            end;
        _ ->
            {reply, ok, State}
    end;

% debugging call: to serialise MFA via this gen_server
handle_call({route, {M, F, A}}, _From, State) ->
    {reply, erlang:apply(M, F, A), State};
handle_call({route, {F, A}}, _From, State) ->
    {reply, erlang:apply(F, A), State};

handle_call(_Request, _From, _S) ->
    error(badarg).

-spec handle_cast(Cast :: {exchange_local, node(), Names :: [{name(), [pid()]}]} |
                          {join, name(), pid()} |
                          {leave, name(), pid()},
                  State :: state()) -> {noreply, state()}.

handle_cast({exchange_local, Node, PidMap}, #state{scope = Scope, remote_pids = Remote} = State) ->
    {noreply, State#state{remote_pids = handle_exchange(Scope, Node, Remote, PidMap)}};

% remote pid joining
handle_cast({join, Group, Pid}, #state{scope = Scope, remote_pids = Remote} = State) when
    node() =/= node(Pid) ->
    join_group(Scope, Group, Pid),
    {noreply, State#state{remote_pids = maps:update_with(Pid, fun (Groups) -> [Group | Groups] end, [Group], Remote)}};

% remote pid leaving
handle_cast({leave, Group, Pid}, #state{scope = Scope, remote_pids = Remote} = State) when
    node() =/= node(Pid) ->
    case maps:find(Pid, Remote) of
        error ->
            % race condition: process left before 'exchange'
            {noreply, State};
        {ok, [Group]} ->
            leave_group(Scope, Group, Pid),
            {noreply, State#state{remote_pids = maps:remove(Pid, Remote)}};
        {ok, Groups} ->
            leave_group(Scope, Group, Pid),
            {noreply, State#state{remote_pids = maps:put(Pid, lists:delete(Group, Groups), Remote)}}
    end;

% what was it?
handle_cast(_, _State) ->
    error(badarg).

-spec handle_info(Tuple :: tuple(), State :: state()) ->
    {'noreply', state()}.

handle_info({'DOWN', MRef, process, Pid, _Info}, #state{scope = Scope, monitors = Monitors} = State) when node(Pid) =:= node() ->
    {{MRef, Groups}, NewMons} = maps:take(Pid, Monitors),
    [leave_local_group(Scope, Group, Pid) || Group <- Groups],
    % send update to all nodes
    [broadcast(Scope, {leave, Group, Pid}) || Group <- Groups],
    {noreply, State#state{monitors = NewMons}};

% nodedown: remove pids from that node
handle_info({nodedown, Node}, #state{scope = Scope, remote_pids = Remote} = State) ->
    NewRemote = maps:filter(
        fun (Pid, Groups) when node(Pid) =:= Node ->
                [leave_group(Scope, Group, Pid) || Group <- Groups],
                false;
            (_Pid, _Group) ->
                true
        end,
        Remote
    ),
    {noreply, State#state{remote_pids = NewRemote}};

% nodeup: send local pids to remote
handle_info({nodeup, Node}, #state{scope = Scope, monitors = Monitors} = State) ->
    gen_server:cast({Scope, Node}, {exchange_local, node(), pid_map(Monitors)}),
    {noreply, State};

% same as nodeup, but for spg server
handle_info({new_pg2, Node}, #state{scope = Scope, monitors = Monitors} = State) ->
    gen_server:cast({Scope, Node}, {exchange_local, node(), pid_map(Monitors)}),
    {noreply, State};

handle_info(_Info, _State) ->
    error(badarg).

-spec terminate(Reason :: any(), State :: state()) -> 'ok'.
terminate(_Reason, #state{scope = Scope}) ->
    true = ets:delete(Scope).

%%--------------------------------------------------------------------
%% Internal implementation

%% Handling exchange is slow, if pid is allowed to join multiple groups,
%%  or join the same group multiple times.
%% Putting the limit (when pid cannot join multiple times) can greatly
%%  simplify implementation, and speed it up as well.
handle_exchange(Scope, Node, Remote, PidMap) ->
    % filter all pids from that node
    {Dirty, Clean} = maps:fold(
        fun (Pid, Gs, {D, C}) when node(Pid) =:= Node ->
            {[{Pid, Gs} | D], maps:remove(Pid, C)};
            (_, _, Acc) ->
            Acc
        end,
        {[], Remote}, Remote
    ),
    %
    % now Dirty contains old PidMap, and PidMap is a new one, and all we need to do is
    %   to find a difference in Groups for every pid.
    Added = lists:foldl(
        fun ({P, Old}, Left) ->
            case maps:take(P, Left) of
                {Old, Left1} ->
                    Left1;
                {New, Left1} ->
                    % here either add or remove pid from groups, New vs Gs.
                    sync_lists(Scope, P, lists:sort(Old), lists:sort(New)),
                    Left1;
                error ->
                    % just remove pid from groups
                    [leave_group(Scope, G, P) || G <- Old],
                    Left
            end
        end, PidMap, Dirty),
    % added pids go directly to groups
    [[join_group(Scope, G, Pid) || G <- Gs] || {Pid, Gs} <- maps:to_list(Added)],
    % merge in new PidMap
    maps:merge(Clean, PidMap).

sync_lists(_Scope, _P, [], []) ->
    ok;
sync_lists(Scope, P, Old, []) ->
    [leave_group(Scope, G, P) || G <- Old];
sync_lists(Scope, P, [], New) ->
    [join_group(Scope, G, P) || G <- New];
% skip same groups
sync_lists(Scope, P, [G|T1], [G|T2]) ->
    sync_lists(Scope, P, T1, T2);
% either leave or join - depending on what sorts earlier
sync_lists(Scope, P, [G1|T1], [G2|_] = T2) when G1 < G2 ->
    leave_group(Scope, G1, P),
    sync_lists(Scope, P, T1, T2);
sync_lists(Scope, P, T1, [G2|T2]) ->
    leave_group(Scope, G2, P),
    sync_lists(Scope, P, T1, T2).

join_local_group(Scope, Name, Pid) ->
    case ets:lookup(Scope, Name) of
        [{Name, All, Local}] ->
            ets:insert(Scope, {Name, [Pid | All], [Pid | Local]});
        [] ->
            ets:insert(Scope, {Name, [Pid], [Pid]})
    end.

join_group(Scope, Name, Pid) ->
    case ets:lookup(Scope, Name) of
        [{Name, All, Local}] ->
            ets:insert(Scope, {Name, [Pid | All], Local});
        [] ->
            ets:insert(Scope, {Name, [Pid], []})
    end.

leave_local_group(Scope, Name, Pid) ->
    case ets:lookup(Scope, Name) of
        [{Name, [Pid], [Pid]}] ->
            ets:delete(Scope, Name);
        [{Name, All, Local}] ->
            ets:insert(Scope, {Name, lists:delete(Pid,All), lists:delete(Pid, Local)});
        [] ->
            % rare race condition when 'DOWN' from monitor stays in msg queue while process is leave-ing.
            true
    end.

leave_group(Scope, Name, Pid) ->
    case ets:lookup(Scope, Name) of
        [{Name, [Pid], []}] ->
            ets:delete(Scope, Name);
        [{Name, All, Local}] ->
            ets:insert(Scope, {Name, lists:delete(Pid,All), Local})
    end.

pid_map(Monitors) ->
    maps:map(fun (_Pid, {_, Groups}) -> Groups end, Monitors).

broadcast(Scope, Msg) ->
    do_broadcast(nodes(), Scope, {'$gen_cast', Msg}).

do_broadcast([], _Scope, _Msg) ->
    ok;
do_broadcast([Node | Tail], Scope, Msg) ->
    % do not use 'nosuspend' here, as it will lead to missing
    %   join/leave messages when dist buffer is full
    erlang:send({Scope, Node}, Msg, [noconnect]),
    do_broadcast(Tail, Scope, Msg).
