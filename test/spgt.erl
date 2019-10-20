%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%%     Common Test helpers (start/stop nodes & simple RPC to
%%%         partitioned nodes).
%%% @end
%%% -------------------------------------------------------------------
-module(spgt).
-author("maximfca@gmail.com").

%% API
-export([
    spawn/0,
    spawn/1,
    stop_proc/1,
    start_scope/1,
    stop_scope/1,
    spawn_node/2,
    spawn_node/3,
    stop_node/1
]).

%% @doc Starts process that waits forever.
-spec spawn() -> pid().
spawn() ->
    erlang:spawn(forever()).

%% @doc Starts process that waits forever on node Node.
-spec spawn(node()) -> pid().
spawn(Node) ->
    erlang:spawn(Node, forever()).

%% @doc
%% Starts the server, not supervised.
-spec start_scope(Scope :: atom()) -> {ok, pid()} | {error, any()}.
start_scope(Scope) when is_atom(Scope), Scope =/= undefined ->
    gen_server:start({local, Scope}, spg, [Scope], []).

%% @doc
%% Stops the unsupervised server.
-spec stop_scope(Scope :: atom()) -> {ok, pid()} | {error, any()}.
stop_scope(Scope) when is_atom(Scope), Scope =/= undefined ->
    gen_server:stop(Scope).

%% @doc Kills process Pid and waits for it to exit using monitor,
%%      and yields after (for 1 ms).
-spec stop_proc(pid()) -> ok.
stop_proc(Pid) ->
    monitor(process, Pid),
    erlang:exit(Pid, kill),
    receive
        {'DOWN', _MRef, process, Pid, _Info} ->
            timer:sleep(1)
    end.

%% @doc starts peer node on this host.
%% Returns spawned node name, and a gen_tcp socket to talk to it using ?MODULE:rpc.
%% When Scope is undefined, no spg scope is started.
%% Name: short node name (no @host.domain allowed).
-spec spawn_node(Scope :: atom(), Name :: atom()) -> gen_node:dest().
spawn_node(Scope, Name) ->
    spawn_node(Scope, Name, true).

-spec spawn_node(Scope :: atom(), Node :: atom(), AutoConnect :: boolean()) -> gen_node:dest().
spawn_node(Scope, Name, AutoConnect) ->
    {ok, Peer} = local_node:start_link(Name, #{auto_connect => AutoConnect,
        connection => {listen, undefined, undefined},
        connect_all => false, code_path => [filename:dirname(code:which(spg))]}),
    {ok, _SpgPid} = gen_node:rpc(Peer, gen_server, start, [{local, Scope}, spg, [Scope], []]),
    Node = gen_node:get_node(Peer),
    {Node, Peer}.

%% @doc Stops the node previously started with spawn_node,
%%      and also closes the RPC socket.
-spec stop_node({node(), gen_tcp:socket()}) -> true.
stop_node({Node, Peer}) when Node =/= node() ->
    gen_node:stop(Peer),
    true.

forever() ->
    fun() -> receive after infinity -> ok end end.
