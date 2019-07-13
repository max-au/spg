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
    control/0,
    spawn/0,
    spawn/1,
    stop_proc/1,
    rpc/4,
    spawn_node/2,
    stop_node/1
]).


control() ->
    Control = self(),
    erlang:spawn(fun () -> server(Control) end),
    Port = receive {port, Port0} -> Port0 end,
    {ok, Host} = inet:gethostname(),
    {Host, Port}.

server(Control) ->
    {ok, Listen} = gen_tcp:listen(0, [{mode, binary}, {packet, 4}]),
    {ok, Port} = inet:port(Listen),
    Control ! {port, Port},
    {ok, Sock} = gen_tcp:accept(Listen),
    server_loop(Sock).

server_loop(Sock) ->
    inet:setopts(Sock, [{active, once}]),
    receive
        {tcp, Sock, Data} ->
            {call, M, F, A} = binary_to_term(Data),
            Ret = (catch erlang:apply(M, F, A)),
            ok = gen_tcp:send(Sock, term_to_binary({reply, Ret})),
            server_loop(Sock);
        {tcp_closed, Sock} ->
            ok
    end.

spawn() ->
    erlang:spawn(forever()).

spawn(Node) ->
    erlang:spawn(Node, forever()).

stop_proc(Pid) ->
    monitor(process, Pid),
    erlang:exit(Pid, kill),
    receive
        {'DOWN', _MRef, process, Pid, _Info} ->
            timer:sleep(1)
    end.

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

-spec spawn_node(atom(), atom()) -> {node(), gen_tcp:socket()}.
spawn_node(TestCase, Name) ->
    Self = self(),
    erlang:spawn(
        fun () ->
            Pa = filename:dirname(code:which(?MODULE)),
            Pa2 = filename:dirname(code:which(spg)),
            Args = "-connect_all false -kernel dist_auto_connect never -noshell -pa " ++ Pa ++ " -pa " ++ Pa2,
            {ok, Node} = test_server:start_node(Name, peer, [{args, Args}]),
            Self ! {started, Node},
            receive
                stop ->
                    ok
            end
        end),
    receive
        {started, Node} ->
            {ok, _Pid} = retry_rpc(Node, spg, start_link, [TestCase], 10),
            {Host, Port} = rpc:call(Node, ?MODULE, control, []),
            {ok, Socket} = gen_tcp:connect(Host, Port, [{active, false}, {mode, binary}, {packet, 4}]),
            {Node, Socket}
    end.

retry_rpc(Node, M, F, A, 1) ->
    rpc:call(Node, M, F, A);
retry_rpc(Node, M, F, A, Count) ->
    case rpc:call(Node, M, F, A) of
        {badrpc, _} ->
            timer:sleep(100),
            retry_rpc(Node, M, F, A, Count - 1);
        Other -> Other
    end.

stop_node(Node) ->
    true = test_server:stop_node(Node).

forever() ->
    fun() -> receive after infinity -> ok end end.
