%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%% EPMD client, implementing distributed discovery.
%%%
%%%  Terms:
%%%     * NodeInfo - Node, Host, AddressFamily
%%%     * AddrInfo - IP Address, Port, Protocol
%%%     * PeerInfo - List of ways to connect to this node [{NodeInfo, AddrInfo}]
%%%     * initiator - node that ran "net_kernel:connect_node(Target)",
%%%                  Initiator is PeerInfo
%%%     * target - node that needs to be connected to initiator, just a node name,
%%%                  Target is a node name
%%%     * forwarder - node that is neither initiator nor target, but participates in the cluster
%%%
%%%  Algorithm:
%%%     0. Every node knows it's own PeerInfo - [{NodeInfo, AddrInfo}].
%%%     1. Initiator wants to connect to Target. Initiator asks epmd_client "where is Target".
%%%         When Target is present in the Cache, it is returned, and connection proceeds from Initiator.
%%%     2. When Target is not in the cache, but already connected, it is a race condition, and error is returned
%%%         if Initiator attempts to call address_please.
%%%     3. When Initiator does not know any nodes in the cluster, it performs "Bootstrap" to join the cluster.
%%%         DNS resolution, or multicast, broadcast, anything.
%%%     4. When Initiator knows nodes in the cluster, but Target not in the cache,
%%%         Initiator floods selected (???) connected nodes "please tell Target to connect to me, I am Initiator",
%%%         this message also contains "I have contacted these nodes, so don't forward my request there".
%%%         {connect_please, Target, Initiator, Visited}
%%%     5. Forwarder may know the Target (in the cache, or list of connected nodes), and in this case it sends
%%%         direct request to the Target:
%%%         {connect_please, Initiator}
%%%     6. Forwarder that does not know the Target floods connected nodes (except those already contacted):
%%%         {connect_please, Target, Initiator, MoreVisited}
%%%     7. When Target receives "connect_please", it does net_kernel:connect_node(Initiator).
%%%     8. When any node connects to any node, they exchange NodeInfo/AddrInfo pairs to populate the cache:
%%%         {update, [{NodeInfo, AddrInfo}]}
%%%
%%%  TODO: add caching mechanism for forwarders, to reduce amount of flood messages generated.
%%% @end
-module(epmd_client).
-author("maximfca@gmail.com").


-behaviour(gen_server).

%% EPMD client API
-export([
    start_link/0,
    bootstrap/0,
    names/0,
    names/1,
    register_node/2,
    register_node/3,
    address_please/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2
]).

%% Internal export: should be used for debugging only
-export([log/2]).

-include_lib("kernel/include/logger.hrl").
%-define (LOG_DEBUG(Fmt, Args), epmd_client:log(Fmt, Args)).

%% Allow up to 5 seconds for resolve
-define (RESOLVE_TIMEOUT, 5000).

%% Keep resolved addresses for this long, if there is no direct connection to that node
-define (INDIRECT_CACHE_TTL_SEC, 30).

%%--------------------------------------------------------------------
%%% API
%% @doc
%% Starts the server and links it to calling process.
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

names() ->
    {ok, Host} = inet:gethostname(),
    names(Host).

names(Host) ->
    gen_server:call(?MODULE, {names, Host}).

register_node(Node, Port) ->
    register_node(Node, Port, inet).

register_node(Node, Port, Driver) ->
    gen_server:call(?MODULE, {register, Node, Port, Driver}).

-spec address_please(Name, Host, AddressFamily) -> Success | {error, term()} when
    Name :: string(),
    Host :: string() | inet:ip_address(),
    AddressFamily :: inet | inet6,
    Port :: non_neg_integer(),
    Version :: non_neg_integer(),
    Success :: {ok, inet:ip_address()} | {ok, inet:ip_address(), Port, Version}.

address_please(Name, Host, AddressFamily) ->
    ?LOG_DEBUG("Address, please: ~s@~s (~s)", [Name, Host, AddressFamily]),
    case gen_server:call(?MODULE, {resolve, {Name, Host, AddressFamily}}, ?RESOLVE_TIMEOUT) of
        {error, Reason} ->
            ?LOG_DEBUG("No destination: ~120p", [Reason]),
            {error, Reason};
        {Addr, Port} ->
            ?LOG_DEBUG("Here is your address: ~p:~b", [Addr, Port]),
            {ok, Addr, Port, 5};
        What ->
            ?LOG_DEBUG("WHAAT: ~120p", [What]),
            What
    end.

% Bootstrap: node, host, IP address (+family), protocol, port.
% Example: {'control', "localhost", {127, 0,0, 1}, tcp, 4370}
% Example: {'control', "ipv6.localhost", {0,0,0,0,0,1}, tls, 4371}
%  Node name cannot be undefined.
%  Hostname can be short or long (containing at least one dot), or 'undefined'
%       (meaning inet:gethostname for short & gethostname + domain for longnames)
%  When IP address is 'undefined', it is expected to be resolvable via DNS,
%  When Port is 'undefined', it is 4370 (tcp) or 4371 (tls).
%  Connection type cannot be undefined.
-type bootstrap_info() :: {
    Node :: atom(),
    Host :: string() | undefined,
    Address :: inet:ip_address() | undefined,
    Port :: non_neg_integer() | undefined,
    tcp | tls}.

%% @doc returns a list of bootstrap tuples
-spec bootstrap() -> [bootstrap_info()].
bootstrap() ->
    case application:get_env(kernel, bootstrap) of
        {ok, {M, F, A}} when is_atom(M), is_atom(F), is_list(A) ->
            % call external API providing this list
            erlang:apply(M, F, A);
        {ok, List} when is_list(List) ->
            % must be complete bootstrap list
            [complete_bootstrap_record(L) || L <- List];
        undefined ->
            []
    end.

%%--------------------------------------------------------------------
%%% gen_server callbacks

%% epmd map: {node, host, ipfamily} => {ip, port}, version is always 5.
-type state() :: #{
    {Name :: atom(), Host :: string() | inet:ip_address(), AddressFamily :: inet | inet6} =>
        {{inet:ip_address(), Port :: non_neg_integer()}, Expires :: integer()}
}.

-spec init([]) -> {ok, state()}.
init([]) ->
    % bootstrap cannot happen yet, resolvers are not ready yet, and net_kernel is not started.
    {ok, #{}}.

handle_call({names, _Host}, _From, State) ->
    {reply, {ok, []}, State};

handle_call({register, _Node, _Port, _AddressFamily} = Register, _From, State) ->
    {reply, {ok, rand:uniform(3)}, State, {continue, Register}};

handle_call({resolve, Target}, _From, State) ->
    case maps:find(Target, State) of
        {ok, {Cached, never}} ->
            ?LOG_DEBUG("Cache hit: ~120p => ~120p (never expires)", [Target, Cached]),
            {reply, Cached, State};
        {ok, {Cached, TTL}} ->
            case erlang:system_time(second) of
                Now when Now < TTL ->
                    ?LOG_DEBUG("Cache hit: ~120p => ~120p (~120b)", [Target, Cached, TTL - Now]),
                    {reply, Cached, State};
                _Now ->
                    ?LOG_DEBUG("Cache expired: ~120p => ~120p (~b)", [Target, Cached, TTL - erlang:system_time(second)]),
                    initiate_search(Target, true, maps:remove(Target, State))
            end;
        error ->
            ?LOG_DEBUG("Cache miss: ~120p", [Target]),
            initiate_search(Target, true, State)
    end;
handle_call(_Request, _From, _State) ->
    error(badarg).

%% handling peer AddrInfo caching
%% PeerInfo is a list of {NodeInfo, AddrInfo}
handle_cast({update, PeerInfo}, State) ->
    ?LOG_DEBUG("Peer Info Update: ~120p", [PeerInfo]),
    {noreply, update_cache(PeerInfo, never, State)};

%% Target handling "please, connect" requests.
handle_cast({connect_please, Initiator}, State) ->
    ?LOG_DEBUG("Target: from ~120p", [Initiator]),
    State1 = update_cache(Initiator, ?INDIRECT_CACHE_TTL_SEC, State),
    % form node name out of peer info (TODO: improve PeerInfo list to avoid node name duplication)
    {Name, Host, _} = hd(Initiator),
    Node = list_to_atom(lists:concat([Name, "@", Host])),
    % spawn a separate process to initiate connection, as we don't care about actual status of it
    proc_lib:spawn_link(fun () -> net_kernel:connect_node(Node) end),
    {noreply, State1};

%% Forwarder handling "please, connect"
handle_cast({connect_please, Target, Initiator, Visited}, State) ->
    ?LOG_DEBUG("Forwarder: target ~p, from ~120p, visited ~200p", [Target, Initiator, Visited]),
    connect_please(Target, Initiator, Visited),
    {noreply, State};

handle_cast(Request, State) ->
    ?LOG_DEBUG("BAD CAST: ~120p (~120p)", [Request, State]),
    error(badarg).

% handling peer AddrInfo caching
handle_info({nodeup, NodeUp}, State) when NodeUp =:= node() ->
    % yes we want to ignore our own 'up'
    {noreply, State};
handle_info({nodeup, NodeUp}, State) ->
    ?LOG_DEBUG("NodeUp: ~p", [NodeUp]),
    gen_server:cast({?MODULE, NodeUp}, {update, get_peer_info(State)}),
    {noreply, State};

handle_info({nodedown, NodeDown}, State) ->
    ?LOG_DEBUG("NodeDown: ~p", [NodeDown]),
    % set up cache expiration (look for both inet/inet6 families)
    [Node, Host] = string:split(atom_to_list(NodeDown), "@"),
    NewTTL = erlang:system_time(seconds) + ?INDIRECT_CACHE_TTL_SEC,
    State1 = update_ttl({Node, Host, inet6}, NewTTL, update_ttl({Node, Host, inet}, NewTTL, State)),
    {noreply, State1};

handle_info(Info, State) ->
    ?LOG_DEBUG("BAD INFO: ~120p (~120p)", [Info, State]),
    error(badarg).

handle_continue({register, Node0, Port, _DistProtocol}, State) ->
    % start listening for node up/down events, this is safe now with newer OTP versions,
    %   as what it actually does, is an undocumented call to process_flag BIF.
    ok = net_kernel:monitor_nodes(true),
    ?LOG_DEBUG("Registered ~s:~b", [Node0, Port]),
    Node = atom_to_list(Node0),
    Host = get_local_host(),
    % pick up IP addresses
    State1 = lists:foldl(
        fun (IPAddr, Cache) ->
            Cache#{{Node, Host, address_family(IPAddr)} => {{IPAddr, Port}, never}}
        end,
        State, get_external_addresses(Host)),
    % this cache entry never expires
    ?LOG_DEBUG("Cached (no TTL): ~200p", [State1]),
    {noreply, State1}.

%%--------------------------------------------------------------------
%% Internal implementation

-include_lib("kernel/include/inet.hrl").

get_external_addresses(Host) ->
    case application:get_env(kernel, inet_dist_use_interface) of
        {ok, Addr} ->
            Addr;
        undefined ->
            % guess the external IP addresses
            %% TODO: better detection would be helpful here
            AddrList = lists:concat([Addrs ||
                {ok, #hostent{h_addr_list = Addrs}} <- [inet:gethostbyname(Host, inet), inet:gethostbyname(Host,inet6)]]),
            {External, Local} = lists:partition(fun is_external/1, AddrList),
            ?LOG_DEBUG("External for ~p: ~200p", [Host, External ++ Local]),
            External ++ Local
    end.

is_external({127, _, _, _}) ->
    false;
is_external({_, _, _, _, _, _, _, 1}) ->
    false;
is_external(_) ->
    true.

initiate_search({Node, Host, _Family} = Target, AllowBootstrap, Cache) ->
    case connect_please(list_to_atom(lists:concat([Node, "@", Host])), get_peer_info(Cache), [node()]) of
        need_bootstrap when AllowBootstrap =:= true ->
            initiate_search(Target, false, resolve_bootstrap(Cache));
        need_bootstrap ->
            {reply, {error, need_bootstrap}, Cache};
        ok ->
            {reply, {error, need_discovery}, Cache}
    end.

update_cache([], _TTL, Cache) ->
    Cache;
update_cache([{HostInfo, AddrInfo} | Tail], TTL, Cache) ->
    ?LOG_DEBUG("Peer ~120p => ~120p (TTL ~p)", [HostInfo, AddrInfo, TTL]),
    update_cache(Tail, TTL, Cache#{HostInfo => {AddrInfo, TTL}}).

get_peer_info(Cache) ->
    [Node, Host] = string:split(atom_to_list(node()), "@"),
    PI = [begin {AddrInfo, _} = maps:get(HostInfo, Cache), {HostInfo, AddrInfo} end ||
        HostInfo <- [{Node, Host, inet}, {Node, Host, inet6}], is_map_key(HostInfo, Cache)],
    PI.

update_ttl(Key, NewTTL, Map) ->
    case maps:find(Key, Map) of
        error ->
            Map;
        {ok, Value} ->
            Map#{Key => {Value, NewTTL}}
    end.

connect_please(Target, Initiator, Visited) ->
    ?LOG_DEBUG("Connect, please: ~p, initiator: ~200p, visited: ~200p (known ~200p)", [Target, Initiator, Visited, nodes()]),
    % see if Target is connected
    case nodes() of
        [] ->
            need_bootstrap;
        Nodes ->
            case lists:member(Target, Nodes) of
                true ->
                    % send "please connect" to this node only, with no Visited nodes to reduce chattyness
                    ?LOG_DEBUG("Immediate: from ~120p", [Initiator]),
                    gen_server:cast({?MODULE, Target}, {connect_please, Initiator});
                false ->
                    % perform peer flood, excluding already Visited nodes
                    Nodes1 = lists:sort(Nodes),
                    NotVisited = nodes() -- Visited,
                    All = lists:umerge(Nodes1, Visited),
                    ?LOG_DEBUG("Forwarding to ~120p: target ~200p, visited: ~200p", [NotVisited, Target, All]),
                    [gen_server:cast({?MODULE, N}, {connect_please, Target, Initiator, All}) || N <- NotVisited]
            end,
            ok
    end.

get_local_host() ->
    {ok, Host} = inet:gethostname(),
    case net_kernel:longnames() of
        true ->
            lists:concat([Host, ".", inet_db:res_option(domain)]);
        false ->
            Host
    end.

complete_bootstrap_record({Node, undefined, Addr, Port, Type}) ->
    complete_bootstrap_record({Node, get_local_host(), Addr, Port, Type});

complete_bootstrap_record({Node, Host, Addr, undefined, tcp}) ->
    {Node, Host, Addr, application:get_env(kernel, inet_dist_listen_min, 4370), tcp};

complete_bootstrap_record({Node, Host, Addr, undefined, ssl}) ->
    {Node, Host, Addr, 8866, ssl};

complete_bootstrap_record(Complete) ->
    Complete.

resolve_bootstrap(Cache) ->
    lists:foldl(fun resolve_bootstrap_record/2, Cache, bootstrap()).

resolve_bootstrap_record({Node, undefined, Addr, Port, _}, Cache) ->
    Host = get_local_host(),
    ?LOG_DEBUG("Resolving local host ~s@~s:~b", [Node, Host, Port]),
    cache(Node, Host, address_family(Addr), Addr, Port, Cache);
resolve_bootstrap_record({Node, Host, undefined, Port, Type}, Cache) ->
    ?LOG_DEBUG("Resolving ~s@~s:~b (~s)", [Node, Host, Port, Type]),
    NextCache = resolve_inet(Node, Host, Port, inet, Cache),
    resolve_inet(Node, Host, Port, inet6, NextCache);
resolve_bootstrap_record({Node, Host, Addr, Port, _}, Cache) ->
    cache(Node, Host, address_family(Addr), Addr, Port, Cache).

resolve_inet(Node, Host, Port, Family, Cache) ->
    case inet:getaddr(Host, Family, ?RESOLVE_TIMEOUT) of
        {ok, Addr} ->
            ?LOG_DEBUG("Resolved ~s@~s => ~120p:~b", [Node, Host, Addr, Port]),
            cache(Node, Host, Family, Addr, Port, Cache);
        {error, Reason} ->
            ?LOG_DEBUG("Cannot resolve ~s (~s): ~120p", [Host, Family, Reason]),
            Cache
    end.

address_family({_, _, _, _}) ->
    inet;
address_family({_, _, _, _, _, _, _, _}) ->
    inet6.

cache(Node, Host, Family, Addr, Port, Cache) ->
    Cache#{{atom_to_list(Node), Host, Family} => {{Addr, Port}, erlang:system_time(seconds) + ?INDIRECT_CACHE_TTL_SEC}}.

log(Format, Data) ->
    {ok, Fd} = prim_file:open("/tmp/spg.log", [append]),
    Time = calendar:system_time_to_rfc3339(erlang:system_time(second)),
    prim_file:write(Fd, iolist_to_binary(
        io_lib:format("~s ~s: " ++ Format ++ "~n", [Time, node()] ++ Data))),
    prim_file:close(Fd).