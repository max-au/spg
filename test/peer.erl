%% @doc
%% Controller for additional Erlang node running on the same host,
%%  or in a different container/host (e.g. Docker).
%%
%% Contains useful primitives to test Erlang Distribution.
%%
%% == Features ==
%% This module provides an extended variant of OTP 'slave' module.
%% To create and boot peer node with random unique name,
%%  use `start_link/0`.
%% Advanced capabilities are enabled via `start_link/1`.
%% Node may not be distributed, but it is possible
%%  to control the node via out-of-band connection, TCP or
%%  stdin/stdout (useful when network is not available).
%%
%% == Terms ==
%% Origin node - Erlang VM instance that spawns additional nodes.
%% Peer node - a node spawned (and linked to Erlang process on
%%     origin node).
%%
%% Peer node must be connected to the origin node. Options are
%%  * Erlang distribution connection: works similar to OTP 'slave.erl'
%%  * stdin/stdout connection: uses standard io to provide dist-like
%%                             features without starting distribution
%%  * TCP connection: uses TCP dist-like control connection
%%
%% Peer node lifecycle is expected to be the same as controlling process.
%% When connection to the origin node server terminates, peer node
%%  shuts down.
%%
%% Peer node starts detached if there is no standard io connection
%%  requested.
%%
%% To set up TCP connection, use connection => 0 option, this will make
%%  origin node to listen on a random port, and wait in 'booting' state
%%  until peer establishes connection.
%% You can also use connection => 1234 to make origin node listen on port
%%  1234, or specify {Ip, Port} tuple.
%% Setting up standard io OOB connection is done via connection => standard_io.
%%
%% IO is forwarded from peer node to origin via OOB or Erlang distribution.
%% @end
-module(peer).
-author("maximfca@gmail.com").

%% API
-export([
    random_name/0,
    random_name/1,

    start_link/0,
    start_link/1,
    stop/1,

    get_node/1,

    get_state/1,
    wait_boot/2,

    disconnect/2,

    apply/4,
    apply/5,
    send/3
]).


%% Originally gen_statem, but after several iterations it turned
%%  out most messages are allowed in all states.
-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% Internal exports for stdin/stdout, non-distribution RPC, and tests
-export([
    start/0,
    peer_init/1,
    forward/2
]).

%% Convenience type to address specific instance
-type dest() :: lambda:dst().

%% Origin node will listen to the specified port (port 0 is auto-select),
%%  or specified IP/Port, and expect peer node to connect to this port.
-type connection() ::
    Port :: 0..65535 |
    {inet:ip_address(), 0..65535} |
    standard_io.

%% Support for ssh/Docker/...
-type remote() ::
    {ssh, Host :: string()} |
    fun (({OrigExec :: string(), OrigArgs :: [string()]},
        {[inet:ip_address()], Port :: 1..65535}, Options :: start_options()) ->
        {Exec :: string(), Args :: [string()]}).

%% Peer node start options
-type start_options() :: #{
    node => node(),                     %% node name to start
    remote => remote(),                 %% support for SSH/Docker, enables remote node start
    longnames => boolean(),             %% long/short names (default is net_kernel:longnames())
    connection => connection(),         %% out-of-band connection
    args => [string()],                 %% additional command line parameters
    env => [{string(), string()}],      %% additional environment
    wait_boot => false | timeout(),     %% start_link boots the node and waits for
                                        %%  it to be running, 5 sec by default, but can be set to false
    shutdown => brutal_kill | timeout() %% halt the peer brutally, or send init:stop() and wait
}.

%% Peer node states
-type peer_state() :: booting | running | shutting_down.

-export_type([
    dest/0,
    start_options/0,
    peer_state/0
]).

%% Socket connect timeout, for TCP connection.
-define (CONNECT_TIMEOUT, 10000).

%% Socket accept timeout, for TCP connection.
-define (ACCEPT_TIMEOUT, 10000).

%% How long to wait for graceful shutdown.
-define (SHUTDOWN_TIMEOUT, 10000).

%% Synchronous RPC timeout for OOB connection.
-define (SYNC_RPC_TIMEOUT, 5000).

%% Default timeout for peer node to boot.
-define (WAIT_BOOT_TIMEOUT, 15000).

%% @doc creates sufficiently random node name,
%%      attempting to guess short/longnames from the origin node.
%%      Favours longnames if distribution is not started.
-spec random_name() -> node().
random_name() ->
    random_name(longnames()).

%% @doc creates sufficiently random node name,
%%      using OS process ID for origin VM, hash of Erlang
%%      process ID, and system time, milliseconds.
-spec random_name(boolean()) ->
    node() | {error, invalid_node_name | short | long | hostname_not_allowed}.
random_name(LongNames) when is_boolean(LongNames) ->
    OsPid = os:getpid(),
    Uniq = erlang:unique_integer(),
    Node = list_to_atom(lists:concat([?MODULE, "-", OsPid, Uniq])),
    create_name(Node, LongNames, 1).

%% @doc starts a distributed node with random name, on this host,
%%      and waits for that node to boot.
-spec start_link() -> {ok, pid()} | {error, Reason :: term()}.
start_link() ->
    start_link(random_name()).

%% @doc starts peer node.
%%      Accepts additional command line arguments and
%%      other important options, like OOB connection, or stdin/stdout
%%      RPC. Node name 'nonode@nohost' has special meaning, node starts
%%      without distribution. Node name 'undefined' also has special
%%      meaning (dynamic node name, since OTP 23)
-spec start_link(atom() | start_options()) -> {ok, pid()} | {error, Reason :: term()}.
start_link(Node) when is_atom(Node) ->
    start_link(#{node => Node});
start_link(#{node := Node, wait_boot := false} = Options) ->
    maps:find(connection, Options) =:= error andalso not erlang:is_alive() andalso error(not_alive),
    %% normalise long/short name
    Long = maps:get(longnames, Options, longnames()),
    gen_server:start_link(?MODULE, Options#{node => create_name(Node, Long, 1), longnames => Long}, []);
start_link(#{wait_boot := false} = Options) ->
    maps:find(connection, Options) =:= error andalso not erlang:is_alive() andalso error(not_alive),
    %% no name
    gen_server:start_link(?MODULE, Options, []);
start_link(Options) ->
    case start_link(Options#{wait_boot => false}) of
        {ok, Pid} ->
            try
                ok = wait_boot(Pid, maps:get(wait_boot, Options, ?WAIT_BOOT_TIMEOUT)),
                {ok, Pid}
            catch
                exit:timeout ->
                    gen_server:stop(Pid),
                    {error, {boot, timeout}}
            end;
        Error ->
            Error
    end.

%% @doc Stops controlling process, shutting down peer node synchronously
-spec stop(dest()) -> ok.
stop(Dest) ->
    gen_server:stop(Dest).

%% @doc returns full node name, e.g. 'node@localhost'.
-spec get_node(Dest :: dest()) -> node().
get_node(Dest) ->
    gen_server:call(Dest, get_node).

%% @doc returns peer node state.
-spec get_state(Dest :: dest()) -> peer_state().
get_state(Dest) ->
    gen_server:call(Dest, get_state).

%% @doc waits until peer node(s) boot sequence completes.
%%      Returns ok if all peers started successfully.
-spec wait_boot(dest() | [dest()], timeout()) -> Result | #{dest() => Result} when
    Result :: ok.
wait_boot(Dests, Timeout) when is_list(Dests) ->
    %% spawn middle-man process per request to avoid late replies
    Control = self(),
    ChildMap = [
        begin
            {Pid, MRef} = spawn_monitor(
                fun () ->
                    New = wait_boot(Dest, Timeout),
                    Control ! {self(), Dest, New}
                end),
            {Pid, {MRef, Dest}}
        end || Dest <- Dests],
    wait_states_impl(maps:from_list(ChildMap), #{});
wait_boot(Dest, Timeout) ->
    gen_server:call(Dest, wait_boot, Timeout).

%% @doc Disconnects remote node from Erlang distribution) and waits
%%  for it to be out of cluster. If this node does not have OOB connection,
%%  it will shut down completely.
%%      Use `net_kernel:connect_node` to connect to remote node. If there
%%  is no known moment when remote node is ready to accept connections, it
%%  is possible to use `peer:apply(Peer, net_kernel, connect_node, node())`
%%  to initiate the connection from the other side.
-spec disconnect(Dest :: dest(), Timeout :: pos_integer()) -> ok | timeout.
disconnect(Dest, Timeout) ->
    Node = get_node(Dest),
    erlang:monitor_node(Node, true),
    net_kernel:disconnect(Node),
    receive
        {nodedown, Node} ->
            ok
    after Timeout ->
        erlang:monitor_node(Node, false),
        timeout
    end.

%% @doc Applies M:F(A) remotely, via OOB connection, with default timeout
-spec apply(Dest :: dest(), module(), atom(), [term()]) -> term().
apply(Dest, M, F, A) ->
    ?MODULE:apply(Dest, M, F, A, ?SYNC_RPC_TIMEOUT).

%% @doc Applies M:F(A) remotely, timeout is explicitly specified
-spec apply(Dest :: dest(), module(), atom(), [term()], timeout()) -> term().
apply(Dest, M, F, A, Timeout) ->
    case gen_server:call(Dest, {apply, M, F, A}, Timeout) of
        {ok, Reply} ->
            Reply;
        {Class, {Reason, Stack}} ->
            erlang:raise(Class, Reason, Stack);
        {error, Reason} ->
            erlang:error(Reason)
    end.

%% @doc Sends a message to pid or named process on the peer node
%%  using OOB connection. No delivery guarantee.
-spec send(Dest :: dest(), To :: pid() | atom(), Message :: term()) -> ok.
send(Dest, To, Message) ->
    gen_server:cast(Dest, {send, To, Message}).

%%--------------------------------------------------------------------
%%% gen_statem callbacks

-record(peer_state, {
    options :: start_options(),
    %% oob connection socket/port
    connection :: undefined | port() | gen_tcp:socket(),
    %% listening socket, while waiting for network OOB connection
    listen_socket :: undefined | gen_tcp:socket(),
    %% accumulator for RPC over standard_io
    stdio = <<>> :: binary(),
    %% peer state
    peer_state = booting :: peer_state(),
    %% calls waiting for node to be started
    wait_boot = [] :: [{pid(), reference()}],
    %% counter (reference) for apply calls.
    %% it is not possible to use erlang reference, or pid,
    %%  because it changes when node becomes distributed dynamically.
    seq = 0 :: non_neg_integer(),
    %% outstanding 'apply' calls
    outstanding = #{} :: #{non_neg_integer() => {reference(), pid()}}
}).

-type state() :: #peer_state{}.

-spec init(start_options()) -> {ok, state()}.
init(Options) ->
    process_flag(trap_exit, true), %% need this to ensure terminate/2 is called

    {ListenSocket, Listen} = maybe_listen(Options),
    {Exec, Args} = maybe_remote(command_line(Listen, Options), Listen, Options),

    Env = maps:get(env, Options, []),
    FoundExec = os:find_executable(Exec),
    Port = open_port({spawn_executable, FoundExec}, [{args, Args}, {env, Env}, hide, binary]),

    Detached = maps:find(connection, Options) =/= {ok, standard_io},

    %% close port if running detached
    Conn = if Detached ->
                   erlang:port_close(Port),
                   receive {'EXIT', Port, _} -> undefined end;
               true ->
                   Port
           end,

    %% start async listener if requested
    if ListenSocket =:= undefined ->
            {ok, #peer_state{options = Options, connection = Conn}};
        true ->
            prim_inet:async_accept(ListenSocket, ?ACCEPT_TIMEOUT),
            {ok, #peer_state{options = Options, listen_socket = ListenSocket}}
    end.

%% not connected: no OOB connection available
handle_call({apply, _M, _F, _A}, _From, #peer_state{connection = undefined} = State) ->
    {reply, {error, noconnection}, State};

handle_call({apply, M, F, A}, From,
    #peer_state{connection = Port, options = #{connection := standard_io},
        outstanding = Out, seq = Seq} = State) ->
    origin_to_peer(port, Port, {apply, Seq, M, F, A}),
    {noreply, State#peer_state{outstanding = Out#{Seq => From}, seq = Seq + 1}};

handle_call({apply, M, F, A}, From,
    #peer_state{connection = Socket, outstanding = Out, seq = Seq} = State) ->
    origin_to_peer(tcp, Socket, {apply, Seq, M, F, A}),
    {noreply, State#peer_state{outstanding = Out#{Seq => From}, seq = Seq + 1}};

handle_call(get_node, _From, #peer_state{options = Options} = State) ->
    {reply, maps:get(node, Options, 'nonode@nohost'), State};

handle_call(wait_boot, _From, #peer_state{peer_state = running} = State) ->
    {reply, ok, State};
handle_call(wait_boot, _From, #peer_state{peer_state = shutting_down} = State) ->
    {reply, shutting_down, State};
handle_call(wait_boot, From, #peer_state{peer_state = booting, wait_boot = WB} = State) ->
    {noreply, State#peer_state{wait_boot = [From | WB]}};

handle_call(get_state, _From, #peer_state{peer_state = PeerState} = State) ->
    {reply, PeerState, State}.

handle_cast({send, _Dest, _Message}, #peer_state{connection = undefined} = State) ->
    {noreply, State};

handle_cast({send, Dest, Message},
    #peer_state{connection = Port, options = #{connection := standard_io}} = State) ->
    origin_to_peer(port, Port, {message, Dest, Message}),
    {noreply, State};

handle_cast({_send, Dest, Message}, #peer_state{connection = Socket} = State) ->
    origin_to_peer(tcp, Socket, {message, Dest, Message}),
    {noreply, State}.

%%--------------------------------------------------------------------
%% OOB connections handling

%% OOB communications - request or response from peer
handle_info({tcp, Socket, SocketData},  #peer_state{connection = Socket} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, handle_oob_data(tcp, binary_to_term(SocketData), State)};

%% standard_io
handle_info({Port, {data, PortData}}, #peer_state{connection = Port, stdio = PrevBin} = State) ->
    {Str, NewBin} = decode_port_data(PortData, <<>>, PrevBin),
    Str =/= <<>> andalso io:format("~s", [Str]),
    {noreply, handle_port_binary(NewBin, State)};

%% booting: accepted TCP connection from the peer, but it is not yet
%%  complete handshake
handle_info({inet_async, LSock, _Ref, {ok, CliSocket}},
    #peer_state{listen_socket = LSock} = State) ->
    true = inet_db:register_socket(CliSocket, inet_tcp),
    ok = inet:setopts(CliSocket, [{active, once}]),
    catch gen_tcp:close(LSock),
    {noreply, State#peer_state{connection = CliSocket, listen_socket = undefined}};

%% booting: peer notifies via Erlang distribution
handle_info({peer_started, Node, Pid},
    #peer_state{options = #{node := Node}} = State) ->
    receive
        {'EXIT', Pid, normal} ->
            true = erlang:monitor_node(Node, true),
            {noreply, boot_complete(State)}
    end;

%% nodedown: no-oob dist-connected peer node is down, stop the server
handle_info({nodedown, Node},
    #peer_state{options = #{node := Node}, connection = undefined} = State) ->
    {stop, {nodedown, Node}, State#peer_state{peer_state = shutting_down}};

%% port terminated: cannot proceed, stop the server
handle_info({'EXIT', Port, Reason}, #peer_state{connection = Port} = State) ->
    catch erlang:port_close(Port),
    {stop, Reason, State#peer_state{connection = undefined}};

handle_info({tcp_closed, Sock}, #peer_state{connection = Sock} = State) ->
    %% TCP connection closed, no i/o port - assume node is stopped
    catch gen_tcp:close(Sock),
    {stop, normal, State#peer_state{connection = undefined}}.

%%--------------------------------------------------------------------
%% cleanup/termination

-spec terminate(Reason :: term(), state()) -> ok.
terminate(_Reason, #peer_state{connection = Port, options = Options}) ->
    case {maps:get(shutdown, Options, brutal_kill), maps:find(connection, Options)} of
        {brutal_kill, {ok, standard_io}} ->
            catch erlang:port_close(Port);
        {brutal_kill, {ok, _TCP}} ->
            catch gen_tcp:close(Port);
        {Timeout, error} ->
            Node = maps:get(node, Options),
            net_kernel:disconnect(Node),
            Timeout =/= brutal_kill andalso
                receive {nodedown, Node} -> ok after Timeout -> ok end;
        {Timeout, {ok, standard_io}} ->
            origin_to_peer(port, Port, {message, init, {stop, stop}}),
            receive {'EXIT', Port, _Reason2} -> ok after Timeout -> ok end,
            catch erlang:port_close(Port);
        {Timeout, {ok, _TCP}} ->
            origin_to_peer(tcp, Port, {message, init, {stop, stop}}),
            receive {tcp_closed, Port} -> ok after Timeout -> ok end,
            catch catch gen_tcp:close(Port)
    end,
    ok.

%%--------------------------------------------------------------------
%% Internal implementation

%% i/o protocol from origin:
%%  * {io_reply, ...}
%%  * {message, To, Content}
%%  * {apply, From, M, F, A}
%%
%% i/o port protocol, from peer:
%%  * {io_request, From, ReplyAs, Request}
%%  * {message, To, Content}
%%  * {reply, From, ok | throw | error | exit | crash, Result | {Reason, Stack}}

%% Handles bytes coming from OOB connection, forwarding as needed.
handle_oob_data(Kind, {io_request, From, FromRef, IoReq}, #peer_state{connection = Conn} = State) ->
    %% TODO: make i/o completely async
    Reply = {io_reply, From, FromRef, forward_request(IoReq)},
    origin_to_peer(Kind, Conn, Reply),
    State;
handle_oob_data(_Kind, {message, To, Content}, State) ->
    To ! Content,
    State;
handle_oob_data(_Kind, {reply, Seq, Class, Result}, #peer_state{outstanding = Out} = State) ->
    {From, NewOut} = maps:take(Seq, Out),
    gen:reply(From, {Class, Result}),
    State#peer_state{outstanding = NewOut};
handle_oob_data(_Kind, {started, NodeName}, #peer_state{peer_state = booting, options = Options} = State)->
    %% skip node name check if it was not specified
    NodeName = maps:get(node, Options, NodeName),
    boot_complete(State).

forward_request(Req) ->
    GL = group_leader(),
    MRef = erlang:monitor(process, GL),
    GL ! {io_request,self(), MRef, Req},
    receive
        {io_reply, MRef, Reply} ->
            erlang:demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, _, _, _} ->
            {error, terminated}
    end.

%% generic primitive to send data from origin to peer via oob connection
origin_to_peer(tcp, Sock, Term) ->
    ok = gen_tcp:send(Sock, term_to_binary(Term));
origin_to_peer(port, Port, Term) ->
    true = erlang:port_command(Port, term_to_binary(Term)).

%% generic primitive to send data from peer to origin
peer_to_origin(tcp, Sock, Term) ->
    ok = gen_tcp:send(Sock, term_to_binary(Term));
peer_to_origin(port, Port, Term) ->
    %% converts Erlang term to terminal codes
    %% Every binary byte is converted into two 4-bit sequences.
    Bytes = term_to_binary(Term),
    Size = byte_size(Bytes),
    Crc = erlang:crc32(Bytes),
    Total = <<Size:32, Bytes/binary, Crc:32>>,
    Encoded = <<<<3:2, Upper:4, 3:2, 3:2, Lower:4, 3:2>> || <<Upper:4, Lower:4>> <= Total>>,
    true = erlang:port_command(Port, Encoded).

%% convert terminal codes to Erlang term, printing everything that
%%  was detected as text
decode_port_data(<<>>, Str, Bin) ->
    {Str, Bin};
decode_port_data(<<3:2, Quad:4, 3:2, Rest/binary>>, Str, Bin) ->
    decode_port_data(Rest, Str, quad_concat(Bin, Quad, erlang:bit_size(Bin)));
decode_port_data(<<Char:8, Rest/binary>>, Str, Bin) ->
    decode_port_data(Rest, <<Str/binary, Char>>, Bin).

%% concatenates bitstring (by default it's not possible to
%%  add bits to non-byte-aligned binary)
quad_concat(Bin, Quad, Size) when Size rem 8 =:= 4 ->
    <<BinPart:(Size div 8)/binary, Before:4>> = Bin,
    <<BinPart/binary, Before:4, Quad:4>>;
quad_concat(Bin, Quad, _Size) ->
    <<Bin/binary, Quad:4>>.

%% recursively process buffers, potentially changing the state
handle_port_binary(<<Size:32, Payload:Size/binary, Crc:32, Rest/binary>>, State) ->
    Crc = erlang:crc32(Payload),
    Term = binary_to_term(Payload),
    NewState = handle_oob_data(port, Term, State),
    handle_port_binary(Rest, NewState);
handle_port_binary(NewBin, State) ->
    State#peer_state{stdio = NewBin}.

boot_complete(#peer_state{wait_boot = WB} = State) ->
    [gen:reply(To, ok) || To <- WB],
    State#peer_state{wait_boot = [], peer_state = running}.

%% in-place "multi_receive" function, to gather results of
%%  many calls to wait_state.
wait_states_impl(ChildMap, Acc) when ChildMap =:= #{}, Acc =:= #{} ->
    ok;
wait_states_impl(ChildMap, Acc) when ChildMap =:= #{} ->
    Acc;
wait_states_impl(ChildMap, Acc) ->
    receive
        {From, Dest, ok} when is_map_key(From, ChildMap) ->
            {{MRef, Dest}, NewMM} = maps:take(From, ChildMap),
            erlang:demonitor(MRef, [flush]),
            wait_states_impl(NewMM, Acc);
        {'DOWN', MRef, process, Pid, Reason} when is_map_key(Pid, ChildMap) ->
            {{MRef, Dest}, NewMM} = maps:take(MRef, ChildMap),
            wait_states_impl(NewMM, Acc#{Dest => Reason})
    end.

%% check if TCP connection is enabled, and starts listener
maybe_listen(#{connection := Port}) when is_integer(Port) ->
    {ok, LSock} = gen_tcp:listen(Port, [binary, {reuseaddr, true}, {packet, 4}]),
    {ok, WaitPort} = inet:port(LSock),
    {ok, Ifs} = inet:getifaddrs(),
    LocalUp = [proplists:get_value(addr, Opts)
        || {_, Opts} <- Ifs, lists:member(up, proplists:get_value(flags, Opts, []))],
    Local = [Valid || Valid <- LocalUp, is_list(inet:ntoa(Valid))],
    {LSock, {Local, WaitPort}};
maybe_listen(#{connection := {Ip, Port}}) when is_integer(Port) ->
    {ok, LSock} = gen_tcp:listen(Port, [binary, {reuseaddr, true}, {packet, 4}, {ip, Ip}]),
    WaitPort = if Port =:= 0 -> {ok, Dyn} = inet:port(LSock), Dyn; true -> Port end,
    {LSock, {[Ip], WaitPort}};
maybe_listen(_Options) ->
    {undefined, undefined}.

%% callback for SSH/Docker/...
maybe_remote({Exec, Args}, _ListenPort, #{remote := {ssh, Host}}) ->
    {"ssh", [Host, Exec | Args]};
maybe_remote({Exec, Args}, ListenPort, #{remote := Remote} = Options) ->
    Remote({Exec, Args}, ListenPort, Options);
maybe_remote(Original, _ListenPort, _Options) ->
    Original.

command_line(Listen, Options) ->
    %% long/short names
    NameArg = name_arg(maps:find(node, Options), maps:find(longnames, Options)),
    %% additional command line args
    CmdOpts = maps:get(args, Options, []),
    % start command
    StartCmd =
        case Listen of
            undefined when map_get(connection, Options) =:= standard_io ->
                ["-user", atom_to_list(?MODULE)];
            undefined ->
                Origin = atom_to_list(node()),
                ["-detached", "-s", atom_to_list(?MODULE), "peer_init", Origin, start_relay(),
                    "-master", Origin, "--setcookie", atom_to_list(erlang:get_cookie())];
            {Ips, Port} ->
                IpStr = lists:join(",", [inet:ntoa(Ip) || Ip <- Ips]),
                ["-detached", "-user", atom_to_list(?MODULE), "-origin", IpStr, integer_to_list(Port)]
        end,
    %% code path
    CodePath = module_path_args(code:which(?MODULE)),
    %% command line: build
    %% BINDIR environment variable is already set
    %% EMU variable it also set
    Root = code:root_dir(),
    Erts = filename:join(Root, lists:concat(["erts-", erlang:system_info(version)])),
    BinDir = filename:join(Erts, "bin"),
    ErlExec = filename:join(BinDir, "erlexec"),
    {ErlExec, NameArg ++ StartCmd ++ CodePath ++ CmdOpts}.

module_path_args(cover_compiled) ->
    {file, File} = cover:is_compiled(?MODULE),
    ["-pa", filename:absname(filename:dirname(File))];
module_path_args(Path) when is_list(Path) ->
    ["-pa", filename:absname(filename:dirname(Path))];
module_path_args(_) ->
    [].

longnames() ->
    case net_kernel:longnames() of
        ignored ->
            inet_db:res_option(domain) =/= [];
        Bool when is_boolean(Bool) ->
            Bool
    end.

name_arg(error, _) ->
    "";
name_arg({ok, Node}, {ok, false}) ->
    ["-sname", atom_to_list(Node)];
name_arg({ok, Node}, {ok, true}) ->
    ["-name", atom_to_list(Node)];
name_arg(Node, error) ->
    name_arg(Node, {ok, longnames()}).

start_relay() ->
    Control = self(),
    register_unique(spawn_link(
        fun () ->
            receive
                {peer_started, Node} ->
                    Control ! {peer_started, Node, self()}
            end
        end), 0).

register_unique(Pid, Attempt) ->
    Name = list_to_atom(lists:concat([?MODULE, "-", Attempt])),
    try register(Name, Pid), atom_to_list(Name)
    catch error:badarg -> register_unique(Pid, Attempt + 1)
    end.


%%--------------------------------------------------------------------
%% code below taken from net_kernel.erl, to reliably reproduce
%%  how -name or -sname parameters work.

%% Create the node name
create_name(Name, LongNames, Try) ->
    {Head, Host1} = create_hostpart(Name, LongNames),
    case Host1 of
        {ok, HostPart} ->
            {ok, MP} = re:compile("^[0-9A-Za-z_\\-]+$", [unicode]),
            case re:run(Head, MP) of
                {match, _} ->
                    list_to_atom(Head ++ HostPart);
                nomatch ->
                    {error, invalid_node_name}
            end;
        {error, long} when Try =:= 1 ->
            %% It could be we haven't read domain name from resolv file yet
            inet_config:do_load_resolv(os:type(), longnames),
            create_name(Name, LongNames, 0);
        {error, Type} ->
            {error, Type}
    end.

create_hostpart(Name, LongNames) ->
    {Head, Host} = lists:splitwith(fun(C) -> C =/= $@ end, atom_to_list(Name)),
    Host1 = case {Host, LongNames} of
                {[$@, _ | _] = Host, true} ->
                    validate_hostname(Host);
                {[$@, _ | _], false} ->
                    case lists:member($., Host) of
                        true -> {error, short};
                        _ ->
                            validate_hostname(Host)
                    end;
                {_, false} ->
                    case inet_db:gethostname() of
                        H when is_list(H), length(H) > 0 ->
                            {ok, "@" ++ H};
                        _ ->
                            {error, short}
                    end;
                {_, true} ->
                    case {inet_db:gethostname(), inet_db:res_option(domain)} of
                        {H, D} when is_list(D), is_list(H),
                            length(D) > 0, length(H) > 0 ->
                            {ok, "@" ++ H ++ "." ++ D};
                        _ ->
                            {error, long}
                    end
            end,
    {Head, Host1}.

validate_hostname([$@ | HostPart] = Host) ->
    {ok, MP} = re:compile("^[!-ÿ]*$", [unicode]),
    case re:run(HostPart, MP) of
        {match, _} ->
            {ok, Host};
        nomatch ->
            {error, hostname_not_allowed}
    end.

%%--------------------------------------------------------------------
%% peer node implementation

%% @doc Attempts to forward a message from peer node to origin
%%      node via OOB connection. Useful mostly for testing.
-spec forward(Dest :: pid() | atom(), Message :: term()) -> term().
forward(Dest, Message) ->
    group_leader() ! {message, Dest, Message}.

notify_when_started(Kind, Port) ->
    init:notify_when_started(self()) =:= started andalso
        notify_started(Kind, Port).

notify_started(Kind, Port) ->
    peer_to_origin(Kind, Port, {started, node()}).

%%--------------------------------------------------------------------
%% dist connection (same as Erlang 'slave' module)
-spec peer_init([atom(), ...]) -> no_return().
peer_init([Origin, Process]) ->
    {Process, Origin} ! {peer_started, node()},
    spawn(
        fun () ->
            process_flag(trap_exit, true),
            monitor_node(Origin, true),
            receive
                {nodedown, Origin} ->
                    erlang:halt()
            end
        end).

%% I/O redirection: peer side
%% When peer starts detached, there is no connection via stdin/stdout.
%%  It cannot be an Erlang Distribution connection as well, because
%%  for that case -user won't be added to command line.
-spec start() -> pid().
start() ->
    case init:get_argument(origin) of
        {ok, [[Ips, PortAtom]]} ->
            spawn(fun () -> tcp_server(Ips, PortAtom) end);
        error ->
            spawn(fun io_server/0)
    end.

io_server() ->
    process_flag(trap_exit, true),
    Port = erlang:open_port({fd, 0, 1}, [eof, binary]),
    register(user, self()),
    group_leader(self(), self()),
    notify_when_started(port, Port),
    io_server_loop(port, Port, #{}, #{}).

tcp_server(Ips, PortAtom) ->
    Port = list_to_integer(PortAtom),
    IpList = [begin {ok, Addr} = inet:parse_address(Ip), Addr end
        || Ip <- string:lexemes(Ips, ",")],
    spawn(fun () -> tcp_init(IpList, Port) end).

tcp_init(IpList, Port) ->
    try
        Sock = loop_connect(IpList, Port),
        register(user, self()),
        erlang:group_leader(self(), self()),
        notify_when_started(tcp, Sock),
        io_server_loop(tcp, Sock, #{}, #{})
    catch
        Class:Reason:Stack ->
            io:format(standard_io, "TCP connection failed: ~s:~p~n~120p~n", [Class, Reason, Stack]),
            erlang:halt(1)
    end.

loop_connect([], _Port) ->
    error(noconnection);
loop_connect([Ip | More], Port) ->
    case gen_tcp:connect(Ip, Port, [binary, {packet, 4}], ?CONNECT_TIMEOUT) of
        {ok, Sock} ->
            Sock;
        _Error ->
            loop_connect(More, Port)
    end.

%% Message protocol between peers
io_server_loop(Kind, Port, Refs, Out) ->
    receive
        {io_request, From, ReplyAs, Request} when is_pid(From) ->
            %% i/o request from this node, forward it to origin
            peer_to_origin(Kind, Port, {io_request, From, ReplyAs, Request}),
            io_server_loop(Kind, Port, Refs, Out);
        {Port, {data, Bytes}} when Kind =:= port ->
            %% bytes received from Port, they can never be malformed,
            %%  but they can be somewhat scattered
            handle_peer_oob(Kind, Port, binary_to_term(Bytes), Refs, Out);
        {Port, eof} when Kind =:= port ->
            %% stdin closed, if there is no active OOB, stop the node
            erlang:halt(1);
        {'EXIT', Port, badsig} when Kind =:= port ->
            %% ignore badsig (what is it?)
            io_server_loop(Kind, Port, Refs, Out);
        {'EXIT', Port, _Reason} when Kind =:= port ->
            %% stdin closed, if there is no active OOB, stop the node
            erlang:halt(1);
        {tcp, Port, Data} when Kind =:= tcp ->
            ok = inet:setopts(Port, [{active, once}]), %% flow control
            handle_peer_oob(Kind, Port, binary_to_term(Data), Refs, Out);
        {tcp_closed, Port} when Kind =:= tcp ->
            %% TCP connection closed, time to shut down
            erlang:halt(1);
        {reply, Seq, Class, Reply} when is_integer(Seq), is_map_key(Seq, Out) ->
            %% stdin/stdout RPC
            {CallerRef, Out2} = maps:take(Seq, Out),
            Refs2 = maps:remove(CallerRef, Refs),
            erlang:demonitor(CallerRef, [flush]),
            peer_to_origin(Kind, Port, {reply, Seq, Class, Reply}),
            io_server_loop(Kind, Port, Refs2, Out2);
        %% stdin/stdout message forwarding
        {message, To, Content} ->
            peer_to_origin(Kind, Port, {message, To, Content}),
            io_server_loop(Kind, Port, Refs, Out);
        {'DOWN', CallerRef, _, _, Reason} ->
            %% this is really not expected to happen, because "do_apply"
            %%  catches all exceptions
            {Seq, Refs3} = maps:take(CallerRef, Refs),
            {CallerRef, Out3} = maps:take(Seq, Out),
            peer_to_origin(Kind, Port, {reply, Seq, crash, Reason}),
            io_server_loop(Kind, Port, Refs3, Out3);
        {init, started} ->
            notify_started(Kind, Port),
            io_server_loop(Kind, Port, Refs, Out);
        _Other ->
            %% below, what is it?
            io_server_loop(Kind, Port, Refs, Out)
    end.

handle_peer_oob(Kind, Port, {io_reply, From, FromRef, Reply}, Refs, Out) ->
    From ! {io_reply, FromRef, Reply},
    io_server_loop(Kind, Port, Refs, Out);
handle_peer_oob(Kind, Port, {apply, Seq, M, F, A}, Refs, Out) ->
    CallerRef = do_apply(Seq, M, F, A),
    io_server_loop(Kind, Port, Refs#{CallerRef => Seq}, Out#{Seq => CallerRef});
handle_peer_oob(Kind, Port, {message, Dest, Message}, Refs, Out) ->
    Dest ! Message,
    io_server_loop(Kind, Port, Refs, Out).

do_apply(Seq, M, F, A) ->
    Proxy = self(),
    {_, CallerRef} = spawn_monitor(
        fun () ->
            %% catch all errors, otherwise emulator will log
            %%  ERROR REPORT when it is not expected
            try
                Proxy ! {reply, Seq, ok, erlang:apply(M, F, A)}
            catch
                Class:Reason:Stack ->
                    Proxy ! {reply, Seq, Class, {Reason, Stack}}
            end
        end),
    CallerRef.
