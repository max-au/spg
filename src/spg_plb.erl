%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc: PLB: probabilistic load balancer.
%%% 
%%%-------------------------------------------------------------------
-module(spg_plb).
-author("maximfca@gmail.com").

%% API
-export([
    start/1,
    start_link/1,

    add/3,
    remove/2,
    cast/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2
]).

-type scope_name() :: atom().

%%--------------------------------------------------------------------
%% @doc
%% Starts the server outside of supervision hierarchy.
-spec start(Scope :: scope_name()) -> {ok, pid()} | {error, any()}.
start(Scope) when is_atom(Scope) ->
    gen_server:start({local, Scope}, ?MODULE, [Scope], []).

%% @doc
%% Starts the server and links it to calling process.
%% Scope name is supplied.
-spec start_link(Scope :: scope_name()) -> {ok, pid()} | {error, any()}.
start_link(Scope) when is_atom(Scope) ->
    gen_server:start_link({local, Scope}, ?MODULE, [Scope], []).

%%--------------------------------------------------------------------
%% API

%% @doc Add a server to load balancer
add(Scope, Pid, Capacity) when is_pid(Pid), is_integer(Capacity), Capacity > 0 ->
    gen_server:call(Scope, {add, Pid, Capacity}).

%% @doc Removes a server from load balancer
remove(Scope, Pid) ->
    gen_server:call(Scope, {remove, Pid}).


%% @doc Route Msg probabilistically
cast(Scope, Msg) ->
    gen_server:cast(Scope, {forward, Msg}).

%%--------------------------------------------------------------------
%% Internal implementation

%% gen_server implementation
-record(spg_plb_state, {
    capacity = 0 :: integer(),
    %% weighted static probabilistic forwarder
    servers = #{} :: #{integer() => pid()},
    weights = #{} :: #{pid() => integer()}
}).

-type state() :: #spg_plb_state{}.

-spec init([Scope :: atom()]) -> {ok, state()}.
init([_Scope]) ->
    {ok, #spg_plb_state{servers = ets:new(lb, [private, ordered_set])}}.

handle_call({add, Pid, Capacity}, _From, State) ->
    NewState = add_impl(Pid, Capacity, State),
    {reply, NewState#spg_plb_state.capacity, NewState};
handle_call({remove, Pid}, _From, State) ->
    NewState = remove_impl(Pid, State),
    {reply, NewState#spg_plb_state.capacity, NewState}.

handle_cast(_Msg, #spg_plb_state{capacity = 0} = State) ->
    %% silently drop, no capacity at all
    {noreply, State};
handle_cast({forward, Msg}, #spg_plb_state{capacity = Cap, servers = Srv} = State) ->
    {To, NewPids} = sample_impl(Cap, Srv),
    gen_server:cast(To, Msg),
    {noreply, State#spg_plb_state{servers = NewPids}}.

%%--------------------------------------------------------------------
%% Internal implementation

add_impl(Pid, Capacity, #spg_plb_state{capacity = Cap, servers = Srv, weights = Weights} = State) ->
    TotalCap = Cap + Capacity,
    true = ets:insert(Srv, {{TotalCap, Pid}}),
    State#spg_plb_state{capacity = TotalCap, weights = Weights#{Pid => Capacity}}.

remove_impl(Pid, #spg_plb_state{weights = Weights} = State) ->
    %% find capacity of this Pid
    NewWeights = maps:remove(Pid, Weights),
    %% linear scan through the map to rebalance all weights
    {NewTotal, NewServers} = maps:fold(
        fun (P, Weight, {Total, Acc}) ->
            Next = Total + Weight,
            {Next, [{{Next, P}} | Acc]}
        end,
        {0, []}, NewWeights),
    NewTab = ets:new(lb, [private, ordered_set]),
    true = ets:insert(NewTab, NewServers),
    State#spg_plb_state{capacity = NewTotal, weights = NewWeights, servers = NewServers}.

%% static probability map: take a random number 0..Capacity and get the closest element in the map
sample_impl(Capacity, Pids) ->
    Sample = rand:uniform(Capacity),
    case ets:next(Pids, {Sample, undefined}) of
        '$end_of_table' ->
            {_Key, Pid} = ets:first(Pids),
            {Pid, Pids};
        {_Key, Pid} ->
            {Pid, Pids}
    end.
