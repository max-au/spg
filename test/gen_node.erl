%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%%     Peer node implementation, capable of starting and joining nodes
%%%      to distribution cluster.
%%% @end
%%% -------------------------------------------------------------------
-module(gen_node).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/3,
    stop/1,
    disconnect/1,
    get_node/1,
    rpc/4,
    call/3,
    cast/3,
    send/3,
    write/2,
    read/2,
    signal/3,
    exec/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% Internal exports (required to set up TCP connection via non-dist method)
-export([
    slave_loop/1
]).

%% gen_server: destination
-type dest() :: pid() | atom() | gen:emgr_name().

% Stopping methods: rpc (init:stop()), close control socket, term os process, kill os process
-type stop_method() :: stop | close | term | kill.

-type oob_connection() ::  {inet:ip_address() | undefined, Port :: non_neg_integer() | undefined}.

%% gen_node: peer node start options
-type start_options() :: #{
    longnames => boolean(),         % net_kernel: long/short names (default is net_kernel:longnames())
    auto_connect => boolean(),      % establish dist connection to the slave node (default is 'false')
    connect_all => boolean(),       % adds "-connect_all false -kernel dist_auto_connect never", default is 'true'
    detached => boolean(),
    % out-of-band connection settings:
    connection => oob_connection(),
    % start: when undefined, starts the default ?MODULE:slave_loop()
    start => string(),
    cmd_line => string(),
    cookie => default | string(),   % default is to take from node(), undefined means 'no cookie'
    code_path => [string()],
    crash_dump_location => default | string(),  % default is CT private logs + node name, may be undefined though
    stop_methods => [stop_method() | atom()]    % additional methods provided by behavour implementations
}.

-export_type([
    dest/0,
    start_options/0,
    stop_method/0
]).

%%% Behaviour definition

%% Peer node termination (additional components, when necessary).
%% Default termination works and verifies socket connection, dist connection,
%%  port connection, or OS pid running.
-callback terminate_node(Node :: node(), Port :: port(), OsPid :: integer(), Method :: stop_method() | atom()) ->
    ok | skip | default | {Port :: port() | undefined, OsPid :: integer() | undefined}.

%% Most important (and non-optional) callback that builds OS cmd line
-callback command_line(Node :: string() | node(), ListenPort :: non_neg_integer() | undefined,
    Options :: start_options()) -> {FullNode :: node(), CmdLine :: string()}.

-optional_callbacks([terminate_node/4]).

%% Default host address: localhost, ipv4.
-define (LOCALHOST, {127, 0, 0, 1}).

% Socket connect
-define (CONNECT_TIMEOUT, 10000).
% Socket accept timeout
-define (ACCEPT_TIMEOUT, 10000).
% Kill timeout: how long to wait for slave to be killed
-define (KILL_TIMEOUT, 3000).
% Synchronous RPC timeout (for connect/disconnect node)
-define (SYNC_RPC_TIMEOUT, 3000).

% Disconnect timeout (must be lower than gen_server:call timeout)
-define (DISCONNECT_TIMEOUT, 5000).

% Added timeout: how much time to give gen_server to respond
-define(ADDED_TIMEOUT, 2000).

% Stop methods available within this one
-define (DEFAULT_STOP_METHODS, [stop, close, term, kill]).

%% @doc starts peer node on this host.
%%      Accepts additional 'erl' command line arguments.
%%      When requested, makes peer node to join distribution cluster.
%%      Started node has code path added for this module, and any
%%          additional modules supplied.
-spec start_link(Node :: atom(), Node :: node(), Options :: start_options()) -> {ok, pid()} | {error, Reason :: term()}.
start_link(Provider, Node, Options) ->
    gen_server:start_link(?MODULE, [Provider, Node, Options], []).

%% @doc Stops the node and waits until it leaves distribution cluster
-spec stop(dest()) -> ok.
stop(Dest) ->
    gen_server:call(Dest, stop, ?DISCONNECT_TIMEOUT + ?CONNECT_TIMEOUT + ?KILL_TIMEOUT + ?ADDED_TIMEOUT).

%% @doc Disconnects remote node and waits for it to be out of cluster
-spec disconnect(Dest :: dest()) -> true | false.
disconnect(Dest) ->
    Node = get_node(Dest),
    disconnect_impl(Node, fun () -> net_kernel:disconnect(Node) end).

%% @doc Executes remote call on the node via TCP socket
%%      Used when dist connection is not available, or
%%      when it's undesirable to use one.
-spec rpc(Dest :: dest(), module(), atom(), [term()]) -> term().
rpc(Dest, M, F, A) ->
    case gen_server:call(Dest, {rpc, M, F, A}) of
        {ok, Reply} ->
            Reply;
        {exit, Reason} ->
            exit(Reason)
    end.

%% @doc Executes gen_server:call on a remote server.
-spec call(Dest :: dest(), Process :: pid() | atom(), Request :: term()) -> term().
call(Dest, Process, Request) ->
    case gen_server:call(Dest, {call, Process, Request}) of
        {ok, Reply} ->
            Reply;
        {exit, Reason} ->
            exit(Reason)
    end.

%% @doc Casts a message to pid or named process on the peer node.
-spec cast(Dest :: dest(), Process :: pid() | atom(), Message :: term()) -> ok.
cast(Dest, Process, Message) ->
    gen_server:cast(Dest, {cast, Process, Message}).

%% @doc Sends a message to pid or named process on the peer node.
-spec send(Dest :: dest(), Process :: pid() | atom(), Message :: term()) -> ok.
send(Dest, Process, Message) ->
    gen_server:cast(Dest, {send, Process, Message}).

%% @doc returns node name for this server
-spec get_node(Dest :: dest()) -> node().
get_node(Dest) ->
    gen_server:call(Dest, get_node).

%% @doc writes binary to the port
-spec write(Dest :: dest(), What :: binary()) -> ok.
write(Dest, What) ->
    gen_server:call(Dest, {write, What}).

%% @doc reads a single line and returns it
-spec read(Dest :: dest(), line) -> {value, binary()} | empty.
read(Dest, line) ->
    gen_server:call(Dest, {read, line}).

%% @doc sends OS signal to os pid
-spec signal(0 | term | kill, OsPid :: integer(), Timeout :: non_neg_integer()) -> integer() | timeout().
signal(term, OsPid, Timeout) ->
    kill_impl("-SIGTERM", OsPid, Timeout);
signal(kill, OsPid, Timeout) ->
    kill_impl("-SIGKILL", OsPid, Timeout);
signal(0, OsPid, Timeout) ->
    kill_impl("-0", OsPid, Timeout).

%% @doc Executes just like os:cmd, but also returns exit code (errorlevel)
exec(What) ->
    exec_impl(What, undefined, ?KILL_TIMEOUT).

%% @doc waits for OS process to terminate
wait_os_process_exit(OsPid, Timeout) ->
    wait_exit(OsPid, erlang:system_time(millisecond) + Timeout).


%%--------------------------------------------------------------------
%%% gen_server callbacks

-record(state, {
    provider :: module(),
    node :: node(),
    socket :: gen_tcp:socket(),
    port :: port(),
    os_pid :: integer(),
    stop_methods :: [stop_method() | atom()],
    output :: string()  % stdout & stderr combined
}).

init([Provider, Node0, Options]) ->
    {Node, Socket, Port, OsPid} = start_impl(Provider, Node0, Options),
    {ok, #state{provider = Provider, node = Node, socket = Socket, port = Port, os_pid = OsPid,
        stop_methods = maps:get(stop_methods, Options, ?DEFAULT_STOP_METHODS),
        output = queue:new()}}.

handle_call({rpc, _M, _F, _A}, _From, #state{socket = undefined} = State) ->
    {reply, {error, not_connected}, State};

handle_call({rpc, M, F, A}, From, #state{socket = Socket} = State) ->
    ok = gen_tcp:send(Socket, term_to_binary({rpc, From, M, F, A})),
    {noreply, State};

handle_call({call, _Dest, _Request}, _From, #state{socket = undefined} = State) ->
    {reply, {error, not_connected}, State};

handle_call({call, Dest, Request}, From, #state{socket = Socket} = State) ->
    ok = gen_tcp:send(Socket, term_to_binary({Dest, {'$gen_call', From, Request}})),
    {noreply, State};

handle_call(stop, _From, State) ->
    case terminate_impl(State) of
        {ok, NewState} ->
            {stop, normal, ok, NewState};
        {error, #state{socket = Socket, port = Port, os_pid = OsPid} = NewState} ->
            {reply, {error, Socket, Port, OsPid}, NewState}
    end;

handle_call(get_node, _From, #state{node = Node} = State) ->
    {reply, Node, State};

handle_call({write, What}, _From, #state{port = Port} = State) ->
    erlang:port_command(Port, What),
    {reply, ok, State};

handle_call({read, line}, _From, #state{output = Output} = State) ->
    {Ret, Q1} = queue:out(Output),
    {reply, Ret, State#state{output = Q1}};

handle_call(_Request, _From, _State) ->
    error(badarg).

handle_cast({cast, _Dest, _Message}, #state{socket = undefined} = State) ->
    {reply, {error, not_connected}, State};

handle_cast({cast, Dest, Message}, #state{socket = Socket} = State) ->
    ok = gen_tcp:send(Socket, term_to_binary({Dest, {'$gen_cast', Message}})),
    {noreply, State};

handle_cast({send, _Dest, _Message}, #state{socket = undefined} = State) ->
    {reply, {error, not_connected}, State};

handle_cast({send, Dest, Message}, #state{socket = Socket} = State) ->
    ok = gen_tcp:send(Socket, term_to_binary({Dest, Message})),
    {noreply, State};

handle_cast(_Request, _State) ->
    error(badarg).

% TCP communications - request or response from peer
handle_info({tcp, Socket, Data}, #state{socket = Socket} = State) ->
    case binary_to_term(Data) of
        {reply, {{Caller, MRef}, Reply}} ->
            Caller ! {MRef, {ok, Reply}};
        {down, {{Caller, MRef}, Reason}} ->
            Caller ! {MRef, {exit, Reason}};
        {Dest, Message} ->
            Dest ! Message
    end,
    {noreply, State};

handle_info({Port, {exit_status,0}}, #state{port = Port} = State) ->
    {noreply, State#state{port = undefined}};

handle_info({Port, {data, {eol, Data}}}, #state{port = Port, output = Output} = State) ->
    {noreply, State#state{output = queue:in(Data, Output)}};

handle_info({nodedown, _Node}, State) ->
    % ignore late messages
    {noreply, State};

handle_info({nodeup, _Node}, State) ->
    % ignore late messages
    {noreply, State};

handle_info(_Info, _State) ->
    error(badarg).

terminate(_Reason, State) ->
    {ok, _State} = terminate_impl(State).

%%--------------------------------------------------------------------
%% Internal implementation

%% Here "Name" is what passed by user,
%       but actual node name may also contain host.
start_impl(Provider, Name, Options) ->
    % start acceptor, if asked to do so
    {ListenSocket, ListenPort} =
        case maps:find(connection, Options) of
            {ok, {Addr, undefined}} ->
                AddrIn = if Addr =:= undefined -> []; true -> {ip_address, Addr} end,
                {ok, LSock} = gen_tcp:listen(0, [binary, {reuseaddr, true}, {packet, 4} | AddrIn]),
                {ok, WaitPort} = inet:port(LSock),
                {LSock, WaitPort};
            {ok, {Addr, WaitPort}} ->
                AddrIn = if Addr =:= undefined -> []; true -> {ip_address, Addr} end,
                {ok, LSock} = gen_tcp:listen(WaitPort, [binary, {reuseaddr, true}, {packet, 4} | AddrIn]),
                {LSock, WaitPort};
            error ->
                {undefined, undefined}
        end,

    % command line generation part
    % also generates the full (expected) node name
    {Node, CmdLine} = Provider:command_line(Name, ListenPort, Options),

    ct:print("CMD (~s): ~n~s", [Node, CmdLine]),

    % do not use test_server:start_node, because it always wants to establish dist connection
    Port = open_port({spawn, CmdLine}, [binary, exit_status, {line, 16384}]),
    {os_pid, OsPid} = erlang:port_info(Port, os_pid),
    % there may be a need in dist connection before accepting TCP one
    NeedSlaveLoop = maps:find(start, Options) =/= error, % Compiler Bug: is_map_key() fails the compiler!
    %
    try
        Socket = init_listen_connection(Node, ListenSocket, NeedSlaveLoop, maps:get(auto_connect, Options, false)),
        {Node, Socket, Port, OsPid}
    catch
        Class:Reason:Stack ->
            StdOut = read_from_port(Port, "", ?KILL_TIMEOUT),
            terminate_impl(#state{provider = Provider, port = Port, os_pid = OsPid,
                stop_methods = maps:get(stop_methods, Options, ?DEFAULT_STOP_METHODS)}),
            erlang:raise(Class, {Reason, StdOut}, Stack)
    after
        catch gen_tcp:close(ListenSocket)
    end.

% control connection is not needed, but AutoConnect is on
init_listen_connection(Node, undefined, _, true) ->
    connect_node_impl(Node, erlang:system_time(millisecond)),
    undefined;

% control connection is not needed, and no AutoConnect
init_listen_connection(_Node, undefined, _, false) ->
    undefined;

% control connection is needed, and slave loop must also be started,
%   dist needs to auto-start
init_listen_connection(Node, ListenSocket, true, true) ->
    connect_node_impl(Node, erlang:system_time(millisecond)),
    true = is_pid(erlang:spawn(Node, ?MODULE, slave_loop, [])),
    connect_oob_socket(ListenSocket, Node);

% control connection is needed, slave loop is not
init_listen_connection(Node, ListenSocket, false, AutoConnect) ->
    Socket = connect_oob_socket(ListenSocket, Node),
    AutoConnect andalso connect_node_impl(Node, erlang:system_time(millisecond) + ?CONNECT_TIMEOUT),
    Socket.

connect_node_impl(Node, Timelimit) ->
    Now = erlang:system_time(millisecond),
    case net_kernel:connect_node(Node) of
        true ->
            ok;
        false when Now > Timelimit ->
            error({auto_connect, Node});
        false ->
            timer:sleep(10),
            connect_node_impl(Node, Timelimit)
    end.

connect_oob_socket(ListenSocket, Node) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket, ?ACCEPT_TIMEOUT),
    % receive a single handshake message, and verify it is the node
    %   we've been waiting for
    receive_node_name(Socket, Node).

receive_node_name(Socket, Node) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {tcp, Socket, Handshake} when is_binary(Handshake) ->
            case binary_to_term(Handshake) of
                {ok, Node} ->
                    % enable bidirectional forwarding
                    inet:setopts(Socket, [{active, true}]),
                    Socket;
                {ok, AnotherNode} ->
                    error({wrong_node, {expected, Node}, {actual, AnotherNode}})
            end
    end.

disconnect_impl(Node, DisconnectFun) ->
    erlang:monitor_node(Node, true),
    DisconnectFun(),
    Disconnected =
        case lists:member(Node, nodes()) of
            true ->
                receive
                    {nodedown, Node} ->
                        true
                after ?DISCONNECT_TIMEOUT ->
                    false
                end;
            false ->
                true
        end,
    erlang:monitor_node(Node, false),
    Disconnected.

% gone through all methods, and it appeared to be working
terminate_impl(#state{stop_methods = [], os_pid = undefined, socket = undefined, port = undefined} = State) ->
    {ok, State};

% enumerated all methods, yet somehow didn't kill it...
terminate_impl(#state{stop_methods = []} = State) ->
    {error, State};

% try the next method
terminate_impl(#state{provider = Provider, node = Node, port = Port,
    os_pid = OsPid, stop_methods = [Method | Next]} = State) ->
    case erlang:function_exported(Provider, terminate_node, 4) of
        true ->
            case Provider:terminate_node(Node, Port, OsPid, Method) of
                ok ->
                    {ok, State};
                skip ->
                    terminate_impl(State#state{stop_methods = Next});
                {NewPort, NewOsPid} ->
                    terminate_impl(State#state{stop_methods = Next, port = NewPort, os_pid = NewOsPid});
                default ->
                    terminate_impl(terminate_default(State#state{stop_methods = Next}, Method))
            end;
        false ->
            % default termination never returns 'skip' or false
            terminate_impl(terminate_default(State#state{stop_methods = Next}, Method))
    end.

% stop: using rpc init:stop() and waiting for it to succeed (node leaves the cluster)
% works only if node is currently connected (yes, I am aware of race conditions)
-spec terminate_default(#state{}, atom()) -> #state{}.
terminate_default(#state{node = Node} = State, stop) ->
    lists:member(Node, nodes()) andalso
        disconnect_impl(Node, fun () -> rpc:call(Node, init, stop, [], ?DISCONNECT_TIMEOUT) end),
    State;

% socket not connected
terminate_default(#state{socket = undefined} = State, close) ->
    State;

% socket may be connected
terminate_default(#state{socket = Socket} = State, close) ->
    catch gen_tcp:close(Socket),
    case wait_exit(State#state.os_pid, erlang:system_time(millisecond) + ?KILL_TIMEOUT) of
        ok ->
            catch erlang:port_close(State#state.port),
            State#state{socket = undefined, os_pid = undefined, port = undefined, stop_methods = []};
        _ ->
            State#state{socket = undefined}
    end;

% port is not connected
terminate_default(#state{port = undefined} = State, port) ->
    State;

% check if os pid is running, and try to kill it
terminate_default(#state{os_pid = OsPid} = State, term) ->
    case is_integer(OsPid) andalso signal(0, OsPid, ?KILL_TIMEOUT) of
        0 ->
            % see if we can terminate it gracefully
            signal(term, OsPid, ?KILL_TIMEOUT),
            % if wait_os_proces_exit returns non-ok, keep the OsPid for further 'kill' signal
            State#state{os_pid = case wait_os_process_exit(OsPid, ?KILL_TIMEOUT) of
                                     ok ->
                                         undefined;
                                     _ ->
                                         OsPid
                                 end};
        _ ->
            State#state{os_pid = undefined}
    end;

terminate_default(#state{os_pid = OsPid} = State, kill) ->
    is_integer(OsPid) andalso signal(0, OsPid, ?KILL_TIMEOUT) =:= 0 andalso
        begin
            signal(kill, OsPid, ?KILL_TIMEOUT),
            wait_os_process_exit(OsPid, ?KILL_TIMEOUT)
        end,
    State#state{os_pid = undefined}.

wait_exit(OsPid, Timelimit) ->
    Now = erlang:system_time(millisecond),
    case filelib:is_dir(lists:concat(["/proc/", OsPid])) of
        true when Now < Timelimit ->
            erlang:display({OsPid, "running"}),
            timer:sleep(50), % may need an adjustment
            wait_exit(OsPid, Timelimit);
        true ->
            timeout;
        false ->
            ok
    end.

kill_impl(Signal, OsPid, Timeout) when is_integer(OsPid) ->
    KillExec = os:find_executable("kill"),
    exec_impl(KillExec, [Signal, integer_to_list(OsPid)], Timeout).

exec_impl(Program, undefined, Timeout) ->
    read_from_port(erlang:open_port({spawn, Program}, [stream, in, eof, hide, exit_status]), "", Timeout);
exec_impl(Program, Args, Timeout) ->
    Port = erlang:open_port({spawn_executable, Program}, [stream, in, eof, hide, exit_status, {args, Args}]),
    read_from_port(Port, "", Timeout).

read_from_port(undefined, Contents, _Timeout) ->
    Contents;
read_from_port(Port, Contents, Timeout) ->
    receive
        {Port, {data, Data}} ->
            read_from_port(Port, [Data | Contents], Timeout);
        {Port, {exit_status, ErrorLevel}} ->
            catch erlang:port_close(Port),
            {ErrorLevel, lists:flatten(lists:reverse(Contents))}
    after Timeout ->
        {timeout, lists:flatten(lists:reverse(Contents))}
    end.

%% @private
-spec slave_loop(CmdLine :: [atom()] | non_neg_integer()) -> no_return().

%% no OOB connection
slave_loop([undefined]) ->
    receive
        forever ->
            ok
    end;

%% OOB-connected variant
slave_loop([PortAtom]) ->
    Port = list_to_integer(atom_to_list(PortAtom)),
    slave_loop(Port);

slave_loop(Port) ->
    try
        {ok, Sock} = gen_tcp:connect(?LOCALHOST, Port, [binary, {packet, 4}], ?CONNECT_TIMEOUT),
        gen_tcp:send(Sock, term_to_binary({ok, node()})),
        server_loop(Sock, #{}, #{})
    catch
        Class:Reason:Stack ->
            io:format("Exception: ~s:~p~n~120p", [Class, Reason, Stack]),
            halt(1)
    end.

%% Message protocol between peers
%% Outstanding calls: Mref => {Proc, Monitor}
server_loop(Sock, Outstanding, Refs) ->
    inet:setopts(Sock, [{active, once}]),
    receive
        {tcp, Sock, Data} ->
            case binary_to_term(Data) of
                {rpc, {Caller, Mref}, M, F, A} ->
                    % do rpc in the spawned process
                    Proxy = self(),
                    {_, CallerRef} = spawn_monitor(
                        fun () ->
                            Result = erlang:apply(M, F, A),
                            Proxy ! {Mref, Result}
                        end),
                    server_loop(Sock, Outstanding#{Mref => {Caller, CallerRef}}, Refs#{CallerRef => Mref});
                {Dest, {'$gen_call', {Caller, Mref}, Request}} ->
                    CallerRef = erlang:monitor(process, Dest),
                    erlang:send(Dest, Request),
                    server_loop(Sock, Outstanding#{Mref => {Caller, CallerRef}}, Refs#{CallerRef => Mref});
                {Dest, {'$gen_cast', Request}} ->
                    try erlang:send(Dest, Request)
                    catch
                        error:_ -> ok
                    end,
                    server_loop(Sock, Outstanding, Refs);
                {Dest, Message} ->
                    Dest ! Message,
                    server_loop(Sock, Outstanding, Refs)
            end;
        {tcp_closed, Sock} ->
            erlang:halt(1);
        {MRef, Reply} when is_reference(MRef), is_map_key(MRef, Outstanding) ->
            {{Caller, CallerRef}, Outstanding2} = maps:take(MRef, Outstanding),
            Refs2 = maps:remove(CallerRef, Refs),
            erlang:demonitor(CallerRef, [flush]),
            ok = gen_tcp:send(Sock, term_to_binary({reply, {{Caller, MRef}, Reply}})),
            server_loop(Sock, Outstanding2, Refs2);
        {'DOWN', CallerRef, _, _, Reason} ->
            {Mref, Refs3} = maps:take(CallerRef, Refs),
            {{Caller, _}, Outstanding3} = maps:take(Mref, Outstanding),
            ok = gen_tcp:send(Sock, term_to_binary({down, {{Caller, Mref}, Reason}})),
            server_loop(Sock, Outstanding3, Refs3)
        % here should go 'timeout' and 'noconnection' for remote node 'DOWN'
    end.
