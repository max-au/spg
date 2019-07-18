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
    rpc/4,
    spawn_node/2,
    stop_node/2
]).

%% Internal exports (required to set up TCP connection via non-dist method)
-export([
    control/1,
    controller/3
]).

-define (LOCALHOST, {127, 0, 0, 1}).

%% @doc Starts process that waits forever.
-spec spawn() -> pid().
spawn() ->
    erlang:spawn(forever()).

%% @doc Starts process that waits forever on node Node.
-spec spawn(node()) -> pid().
spawn(Node) ->
    erlang:spawn(Node, forever()).

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

%% @doc Executes remote call on the node via TCP socket
%%      Used when dist connection is not available, or
%%      when it's undesirable to use one.
-spec rpc(gen_tcp:socket(), module(), atom(), [term()]) -> term().
rpc(Sock, M, F, A) ->
    ok = gen_tcp:send(Sock, term_to_binary({call, M, F, A})),
    inet:setopts(Sock, [{active, once}]),
    receive
        {tcp, Sock, Data} ->
            {reply, Ret} = binary_to_term(Data),
            Ret;
        {tcp_closed, Sock} ->
            error(closed)
    end.

%% @doc starts peer node on this host.
%% Returns spawned node name, and a gen_tcp socket to talk to it using ?MODULE:rpc.
-spec spawn_node(Scope :: atom(), Node :: atom()) -> {node(), gen_tcp:socket()}.
spawn_node(Scope, Name) ->
    Self = self(),
    Controller = erlang:spawn(?MODULE, controller, [Name, Scope, Self]),
    receive
        {'$node_started', Node, Port} ->
            {ok, Socket} = gen_tcp:connect(?LOCALHOST, Port, [{active, false}, {mode, binary}, {packet, 4}]),
            Controller ! {socket, Socket},
            {Node, Socket};
        Other ->
            error({start_node, Name, Other})
    after 60000 ->
        error({start_node, Name, timeout})
    end.

%% @private
-spec controller(atom(), atom(), pid()) -> ok.
controller(Name, Scope, Self) ->
    Pa = filename:dirname(code:which(?MODULE)),
    Pa2 = filename:dirname(code:which(spg)),
    Args = "-connect_all false -kernel dist_auto_connect never -noshell -pa " ++ Pa ++ " -pa " ++ Pa2,
    {ok, Node} = test_server:start_node(Name, peer, [{args, Args}]),
    case rpc:call(Node, ?MODULE, control, [Scope], 5000) of
        {badrpc, nodedown} ->
            Self ! {badrpc, Node},
            ok;
        {Port, _SpgPid} ->
            Self ! {'$node_started', Node, Port},
            controller_wait()
    end.

controller_wait() ->
    Port =
        receive
            {socket, Port0} ->
                Port0
        end,
    MRef = monitor(port, Port),
    receive
        {'DOWN', MRef, port, Port, _Info} ->
            ok
    end.

%% @doc Stops the node previously started with spawn_node,
%%      and also closes the RPC socket.
-spec stop_node(node(), gen_tcp:socket()) -> true.
stop_node(Node, Socket) when Node =/= node() ->
    true = test_server:stop_node(Node),
    Socket =/= undefined andalso gen_tcp:close(Socket),
    true.

forever() ->
    fun() -> receive after infinity -> ok end end.


-spec control(Scope :: atom()) -> {Port :: integer(), pid()}.
control(Scope) ->
    Control = self(),
    erlang:spawn(fun () -> server(Control, Scope) end),
    receive
        {port, Port, SpgPid} ->
            {Port, SpgPid};
        Other ->
            error({error, Other})
    end.

server(Control, Scope) ->
    try
        {ok, Pid} = spg:start(Scope),
        {ok, Listen} = gen_tcp:listen(0, [{mode, binary}, {packet, 4}, {ip, ?LOCALHOST}]),
        {ok, Port} = inet:port(Listen),
        Control ! {port, Port, Pid},
        {ok, Sock} = gen_tcp:accept(Listen),
        server_loop(Sock)
    catch
        Class:Reason:Stack ->
            Control ! {error, {Class, Reason, Stack}}
    end.

server_loop(Sock) ->
    inet:setopts(Sock, [{active, once}]),
    receive
        {tcp, Sock, Data} ->
            {call, M, F, A} = binary_to_term(Data),
            Ret = (catch erlang:apply(M, F, A)),
            ok = gen_tcp:send(Sock, term_to_binary({reply, Ret})),
            server_loop(Sock);
        {tcp_closed, Sock} ->
            erlang:halt(1)
    end.
