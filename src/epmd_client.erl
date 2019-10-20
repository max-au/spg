%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%% EPMD client replacement to use with Scalable Process Groups.
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
    address_please/3,
    resolve/4
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-export([log_append/2]).

%% Allow up to 5 seconds for resolve
-define (RESOLVE_TIMEOUT, 5000).

%% Cache resolves records for 30 seconds
-define (RESOLVE_TTL_SEC, 30).

%%--------------------------------------------------------------------
%%% API
%% @doc
%% Starts the server and links it to calling process.
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    log_append("Starting ~p", [?MODULE]),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

names() ->
    {ok, Host} = inet:gethostname(),
    names(Host).

names(Host) ->
    gen_server:call(?MODULE, {names, Host}).

register_node(Host, Port) ->
    register_node(Host, Port, inet).

register_node(Host, Port, Driver) ->
    log_append("Register: ~s:~p (~s)", [Host, Port, Driver]),
    gen_server:call(?MODULE, {register, Host, Port, Driver}).

-spec address_please(Name, Host, AddressFamily) -> Success | {error, term()} when
    Name :: string(),
    Host :: string() | inet:ip_address(),
    AddressFamily :: inet | inet6,
    Port :: non_neg_integer(),
    Version :: non_neg_integer(),
    Success :: {ok, inet:ip_address()} | {ok, inet:ip_address(), Port, Version}.

address_please(Name, Host, AddressFamily) ->
    log_append("Address, please: ~s@~s (~s)", [Name, Host, AddressFamily]),
    A = gen_server:call(?MODULE, {resolve, {Name, Host, AddressFamily}, []}),
    log_append("Your address: ~s@~s => ~120p", [Name, Host, A]),
    A.

-spec resolve(Name, Host, AddressFamily, Path) -> Success | {error, term()} when
    Name :: string(),
    Host :: string() | inet:ip_address(),
    AddressFamily :: inet | inet6,
    Path :: [atom()],
    Port :: non_neg_integer(),
    Success :: {inet:ip_address(), Port} | {error, {bootstrap_needed, not_found | disconnected}}.
resolve(Name, Host, AddressFamily, Path) ->
    case list_to_atom(lists:concat([Name, "@", Host])) of
        Node when Node =:= node() ->
            log_append("Requested self from ~120p", [Path]),
            % Hack: use net_kernel internal state knowledge to get a list of own addresses
            AllListeners = element(10, sys:get_state(net_kernel)),
            % find the one asked (tcp, and AddressFamily)
            Addresses = [{Addr, Port} ||
                {listen, _Port, _DistCtrlr, {net_address, {Addr, Port}, _Self, tcp, AF}, _DistType} <- AllListeners,
                AF =:= AddressFamily],
            hd(Addresses);
        Node ->
            case lists:member(Node, nodes()) of
                true ->
                    log_append("Requested known node ~p from ~120p", [Node, Path]),
                    % ask node itself
                    rpc:call(Node, ?MODULE, resolve, [Name, Host, AddressFamily, [node() | Path]], ?RESOLVE_TIMEOUT);
                false ->
                    log_append("Resolve indirect: ~s@~s (~s, from ~120p)", [Name, Host, AddressFamily, Path]),
                    case gen_server:call(?MODULE, {resolve, {Name, Host, AddressFamily}, Path}) of
                        {ok, Addr, Port, _Version} ->
                            {Addr, Port};
                        {error, Reason} ->
                            % this is quite bad Erlang code, need to remove nested clauses
                            {error, Reason}
                    end
            end
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

-include_lib("kernel/include/logger.hrl").

%% epmd map: {node, host, ipfamily} => {ip, port}, version is always 5.
-type state() :: #{
    {Name :: atom(), Host :: string() | inet:ip_address(), AddressFamily :: inet | inet6} =>
        {{inet:ip_address(), Port :: non_neg_integer()}, Expires :: integer()}
}.

-spec init([]) -> {ok, state()}.
init([]) ->
    % bootstrap cannot happen yet, because DNS resolvers are
    %   not ready yet.
    {ok, #{}}.

handle_call({names, _Host}, _From, State) ->
    log_append("Names: ~p", [""]),
    {reply, {ok, []}, State};

handle_call({register, _Host, _Port, _AddressFamily}, _From, State) ->
    {reply, {ok, rand:uniform(3)}, State};

handle_call({resolve, Address, Path}, _From, State) ->
    case resolve_address(Address, Path, true, State) of
        {error, Reason, State1} ->
            {reply, {error, Reason}, State1};
        {{Addr, Port}, State1} ->
            {reply, {ok, Addr, Port, 5}, State1}
    end;
handle_call(_Request, _From, _State) ->
    error(badarg).

handle_cast(_Request, _State) ->
    error(badarg).

handle_info(_Info, _State) ->
    error(badarg).

%%--------------------------------------------------------------------
%% Internal implementation

complete_bootstrap_record({Node, undefined, Addr, Port, Type}) ->
    {ok, Host} = inet:gethostname(),
    case net_kernel:longnames() of
        true ->
            complete_bootstrap_record({Node, lists:concat([Host, ".", inet_db:res_option(domain)]), Addr, Port, Type});
        false ->
            complete_bootstrap_record({Node, Host, Addr, Port, Type})
    end;

complete_bootstrap_record({Node, Host, Addr, undefined, tcp}) ->
    {Node, Host, Addr, application:get_env(kernel, inet_dist_listen_min, 4370), tcp};

complete_bootstrap_record({Node, Host, Addr, undefined, ssl}) ->
    {Node, Host, Addr, 8866, ssl};

complete_bootstrap_record(Complete) ->
    Complete.

%% Address resolver assumes all nodes are on the same port,
%%  and ignores node name completely.
resolve_address(Key, Path, AllowBootstrap, Cache) ->
    case maps:find(Key, Cache) of
        {ok, {Cached, TTL}} ->
            case erlang:system_time(second) of
                Now when Now < TTL ->
                    log_append("Cache hit: ~120p => ~120p (~120b)", [Key, Cached, TTL - Now]),
                    {Cached, Cache};
                _Now ->
                    log_append("Cache expired: ~120p => ~120p (~b)", [Key, Cached, TTL - erlang:system_time(second)]),
                    resolve_address(Key, Path, AllowBootstrap, maps:remove(Key, Cache))
            end;
        error ->
            log_append("Cache: no ~200p in ~200p", [Key, Cache]),
            case resolve_indirect(Key, Path) of
                {need_bootstrap, _} when AllowBootstrap =:= true ->
                    Cache1 = resolve_bootstrap(Cache),
                    resolve_address(Key, Path, false, Cache1);
                {need_bootstrap, Reason} ->
                    {error, {need_bootstrap, Reason}, Cache};
                {{_A, Port} = Addr, TTL} when is_integer(Port), is_integer(TTL) ->
                    {Addr, Cache#{Key => {Addr, TTL}}}
            end
    end.

-spec resolve_indirect({Name :: string(), Host :: string(), AddressFamily :: inet | inet6}, Path :: [atom()]) ->
    {{Addr :: inet:ip_address(), Port :: non_neg_integer()}, TTL :: integer()} |
    {need_bootstrap, not_found | disconnected}.
resolve_indirect({Name, Host, AddressFamily}, Path) ->
    Resolvers = nodes() -- Path,
    Path1 = [node() | Path],
    case rpc:multicall(Resolvers, ?MODULE, resolve, [Name, Host, AddressFamily, Path1], ?RESOLVE_TIMEOUT) of
        {[_ | _] = Responses, _} ->
            GoodResponses = [{A, P} || {A, P} <- Responses, is_integer(P), is_tuple(A)],
            case GoodResponses of
                [] ->
                    log_append("Not found: ~s@~s", [Name, Host]),
                    {need_bootstrap, not_found};
                [{Addr, Port} | _] ->
                    log_append("Resolved: ~s@~s => ~120p:~b", [Name, Host, Addr, Port]),
                    {{Addr, Port}, erlang:system_time(seconds) + ?RESOLVE_TTL_SEC}
            end;
        {[], _BadNodes} ->
            log_append("Not connected: ~s@~s (~120p)", [Name, Host, nodes()]),
            {need_bootstrap, disconnected}
    end.

resolve_bootstrap(Cache) ->
    lists:foldl(fun resolve_bootstrap_record/2, Cache, bootstrap()).

resolve_bootstrap_record({Node, undefined, Addr, Port, _}, Cache) ->
    Family = case tuple_size(Addr) of 4 -> inet; 6 -> inet6 end,
    {ok, Host0} = inet:gethostname(),
    Host =
        case net_kernel:longnames() of
            true ->
                lists:concat([Host0, ".", inet_db:res_option(domain)]);
            false ->
                Host0
        end,
    log_append("Resolving local host ~s@~s:~b", [Node, Host, Port]),
    cache(Node, Host, Family, Addr, Port, Cache);
resolve_bootstrap_record({Node, Host, undefined, Port, Type}, Cache) ->
    log_append("Resolving ~s@~s:~b (~s)", [Node, Host, Port, Type]),
    NextCache = resolve_inet(Node, Host, Port, inet, Cache),
    resolve_inet(Node, Host, Port, inet6, NextCache);
resolve_bootstrap_record({Node, Host, Addr, Port, _}, Cache) ->
    Family = case tuple_size(Addr) of 4 -> inet; 6 -> inet6 end,
    cache(Node, Host, Family, Addr, Port, Cache).

resolve_inet(Node, Host, Port, Family, Cache) ->
    case inet:getaddr(Host, Family, ?RESOLVE_TIMEOUT) of
        {ok, Addr} ->
            log_append("Resolved ~s@~s => ~120p:~b", [Node, Host, Addr, Port]),
            cache(Node, Host, Family, Addr, Port, Cache);
        {error, Reason} ->
            log_append("Cannot resolve ~s (~s): ~120p", [Host, Family, Reason]),
            Cache
    end.

cache(Node, Host, Family, Addr, Port, Cache) ->
    Cache#{{atom_to_list(Node), Host, Family} => {{Addr, Port}, erlang:system_time(seconds) + ?RESOLVE_TTL_SEC}}.

log_append(Format, Data) ->
    {ok, Fd} = prim_file:open("/tmp/spg.log", [append]),
    Time = calendar:system_time_to_rfc3339(erlang:system_time(second)),
    prim_file:write(Fd, iolist_to_binary(
        io_lib:format("~s ~s: " ++ Format ++ "~n", [Time, node()] ++ Data))),
    prim_file:close(Fd).