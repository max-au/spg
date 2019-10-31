%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%% Distributed Process Groups
%%% @end
-module(stateless_service).
-author("maximfca@gmail.com").

%% API: alternative process registry
-export([
    start_link/3
]).

%%% gen_server exports
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Service must be an atom
-type service_name() :: atom().

-define(DEFAULT_DISCOVER_TIMEOUT, 10000).

%% @doc
%% Starts the server and links it to calling process.
%% Scope name is passed as a parameter.
%% Control does not return until service is connected to
%%  requested amount of endpoints.
-spec start_link(Scope :: atom(), Name :: service_name(), Count :: non_neg_integer()) ->
    {ok, pid()} | {error, any()}.
start_link(Scope, Name, Count) when is_atom(Scope), is_atom(Name), is_integer(Count) ->
    gen_server:start_link(?MODULE, {Scope, Name, Count, ?DEFAULT_DISCOVER_TIMEOUT}, []).

%%--------------------------------------------------------------------
%%% gen_server implementation

-include_lib("kernel/include/logger.hrl").
%-define (LOG_DEBUG(Fmt, Args), epmd_client:log(Fmt, Args)).

-record(state, {
    %% spg Scope to use
    scope :: atom(),
    %% spg Service to use
    name :: service_name()
}).

-type state() :: #state{}.

-spec init({Scope :: atom(), Name :: atom(), Count :: non_neg_integer()}) -> {ok, state()}.
init({Scope, Name, Count, Timeout}) ->
    % discover & bootstrap
    connect(Scope, Name, Count, erlang:system_time(millisecond) + Timeout),
    ?LOG_DEBUG("Successfully found ~b instances of ~s:~s (~b nodes)",
        [length(spg:get_members(Scope, Name)), Scope, Name, length(nodes())]),
    {ok, #state{scope = Scope, name = Name}}.

handle_call(_Request, _From, _S) ->
    error(badarg).

handle_cast(_, _State) ->
    error(badarg).

handle_info(_Info, _State) ->
    error(badarg).

%%--------------------------------------------------------------------
%% Internal implementation

connect(Scope, Name, Count, Timelimit) ->
    Now = erlang:system_time(millisecond),
    case spg:get_members(Scope, Name) of
        Connected when length(Connected) >= Count ->
            ok;
        Connected when Now > Timelimit ->
            error({timeout, Scope, Name, Count, length(Connected)});
        Connected ->
            % need to discover more "Name" services
            discover(Scope, Name, Connected),
            connect(Scope, Name, Count, Timelimit)
    end.

discover(Scope, Name, Connected) ->
    % check if we've been bootstrapped
    ensure_bootstrap(epmd_client:bootstrap()),
    % give some time to populate tables, TODO: make is better, and less time-dependent
    timer:sleep(500),
    % ask other nodes "who can provide this service"
    %% TODO: do better job other than simply ask 'is spg scope running'
    {Replies, _} = rpc:multicall(spg, get_members, [Scope, Name], 1000),
    AllPids = lists:concat([Reply || Reply <- Replies, is_list(Reply)]),
    AdditionalPids = lists:usort(AllPids) -- Connected,
    AdditionalNodes = [node(Pid) || Pid <- AdditionalPids],
    % let's connect to additional nodes
    ?LOG_DEBUG("Trying to find ~s:~s on ~200p (from ~200p on ~200p)",
        [Scope, Name, AdditionalNodes, AllPids, nodes()]),
    % ignore if additional nodes cannot be connected
    [_ = net_kernel:connect_node(Node1) || Node1 <- AdditionalNodes],
    % wait for 'service up', TODO: implement non-spinning wait
    timer:sleep(500).

ensure_bootstrap([]) ->
    ok;
ensure_bootstrap([{Name, Host, _, Port, _} | Tail]) ->
    ?LOG_DEBUG("Booting from ~s:~p:~b", [Name, Host, Port]),
    % if bootstrap node is not in the list, do the boot
    Node = list_to_atom(lists:concat([Name, "@", Host])),
    Node =/= node() andalso lists:member(Node, nodes()) =:= false andalso
        begin
            case net_kernel:connect_node(Node) of
                true ->
                    ?LOG_DEBUG("Booted from ~p:~b)", [Node, Port]),
                    ok;
                false ->
                    ?LOG_DEBUG("Could not boot from ~p:~b", [Node, Port]),
                    ensure_bootstrap(Tail)
            end
        end.
