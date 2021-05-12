%% @doc
%% Controller for additional Erlang node running on the same host,
%%  or in a different container/host (e.g. Docker).
%%
%% Contains useful primitives to test Erlang Distribution.
%%
%% == Features ==
%% This module provides an extended variant of OTP 'slave' module.
%% To create and boot peer node with random unique name,
%%  use `start_link/0'.
%% Advanced capabilities are enabled via `start_link/1'.
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
    start_link/0,
    start_link/1,
    start/1,
    stop/1,

    random_name/0,
    random_name/1,

    get_node/1,
    get_state/1,
    wait_boot/2,

    call/4,
    call/5,
    send/3
]).

%% Internal: for test_server compatibility only.
-export([parse_args/1]).

%% Could be gen_statem too, with peer_state, but most interactions
%%  are anyway available in all states.
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
    start/0,        %% this function must be named "start", requirement for user.erl
    peer_init/1
]).

%% Convenience type to address specific instance
-type dest() :: atom() | pid() | {local, atom()} | {global, term()} | {'via', Module :: module(), Name :: term()}.

%% Origin node will listen to the specified port (port 0 is auto-select),
%%  or specified IP/Port, and expect peer node to connect to this port.
-type connection() ::
    Port :: 0..65535 |
    {inet:ip_address(), 0..65535} |
    standard_io.

%% Support for starting the node on a different host
%%  (not present): compatibility mode, when hostname is different from
%%       origin host, start as if it is 'rsh'
%%  false: always start the node locally, even if host name is different
%%  rsh: force start via rsh (ssh by default, configured
%%       via command line -rsh switch)
%%  {rsh, "ssh"}: force start the node with "ssh <host>"
%%  {rsh, "ssh", ["-p", "22", "host"]}: start the node with "ssh -p 22 host..."
%%  custom callback:
%%        fun("erl", ["-boot"], {{127, 0, 0, 1}, 3411}, #{name => new}) ->
%%            "docker -it container name -env "CONNECT_TO" 127.0.0.1:3411
-type rsh() ::
    local |
    rsh |
    {rsh, Exec :: string()} |
    {rsh, Exec :: string(), [Args :: string()]} |
    fun ((OrigExec :: string(), OrigArgs :: [string()],
        {[inet:ip_address()], Port :: 1..65535}, Options :: start_options()) ->
        {Exec :: string(), Args :: [string()]}).

%% Peer node start options
-type start_options() :: #{
    name => atom(),                     %% node name (part before @), if not defined, peer
                                        %%  starts in non-distributed mode (requires OOB connection)
    host => string(),                   %% hostname, inet:gethostname() by default
    longnames => boolean(),             %% long/short names (default is net_kernel:longnames())
    register => local |                 %% when defined, register process with the same name as
                global |                %%  the node being started
                {via, module()},        %% otherwise return pid() of the process linked to peer node
                                        %% not present: true if name/host present, false otherwise
    rsh => rsh(),                       %% remote shell to use for starting a node on a different host
    link => boolean(),                  %% false (default): one-way link from origin to peer node,
                                        %%  when peer terminates, origin does not get any signal
                                        %% true: two-way link, when peer terminates, origin process
                                        %%  also terminates
    progname => string(),               %% path to "erl", by default, init:get_argument(progname)
    connection => connection(),         %% out-of-band connection
    args => [string()],                 %% additional command line parameters
    env => [{string(), string()}],      %% additional environment (useful for rsh)
    wait_boot => false | timeout(),     %% boot the node and waits for it to be running, 15 sec by default
    shutdown => brutal_kill | timeout() %% halt the peer brutally (default), or send init:stop() and wait
}.

%% Peer node states
-type peer_state() :: down | booting | running | shutting_down.

-export_type([
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

%% @doc Creates random node name, using "peer" as prefix.
-spec random_name() -> atom().
random_name() ->
    random_name(?MODULE).

%% @doc Creates sufficiently random node name,
%%      using OS process ID for origin VM, resulting name
%%      looks like peer-12345-44222444
-spec random_name(Prefix :: string() | atom()) -> atom().
random_name(Prefix) ->
    OsPid = os:getpid(),
    Uniq = abs(erlang:unique_integer()),
    list_to_atom(lists:concat([Prefix, "-", OsPid, "-", Uniq])).

%% @doc Starts a distributed node with random name, on this host,
%%      and waits for that node to boot. Returns full node name,
%%      registers local process with the same name as peer node.
-spec start_link() -> {ok, node()} | {error, Reason :: term()}.
start_link() ->
    start_link(#{name => random_name()}).

%% @doc Starts peer node, linked to the calling process.
%%      Accepts additional command line arguments and
%%      other important options.
%%      Returns either node name (and registers local process under
%%      the same name), or local pid.
-spec start_link(start_options()) -> {ok, node() | pid()} | {error, Reason}
    when Reason :: timeout.
start_link(Options) ->
    start_impl(Options, start_link).

%% @doc Starts peer node, not linked to the calling process.
-spec start(start_options()) -> {ok, node() | pid()} | {error, Reason}
    when Reason :: timeout.
start(Options) ->
    start_impl(Options, start).

%% @doc Stops controlling process, shutting down peer node synchronously
-spec stop(dest()) -> ok.
stop(Dest) ->
    gen_server:stop(Dest).

%% @doc returns peer node name.
-spec get_node(Dest :: dest()) -> node().
get_node(Dest) ->
    gen_server:call(Dest, get_node).

%% @doc returns peer node state.
-spec get_state(Dest :: dest()) -> peer_state().
get_state(Dest) ->
    gen_server:call(Dest, get_state).

%% @doc Waits until peer node or nodes boot sequence completes.
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

%% @doc Calls M:F(A) remotely, via OOB connection, with default 5 seconds timeout
-spec call(Dest :: dest(), module(), atom(), [term()]) -> term().
call(Dest, M, F, A) ->
    call(Dest, M, F, A, ?SYNC_RPC_TIMEOUT).

%% @doc Call M:F(A) remotely, timeout is explicitly specified
-spec call(Dest :: dest(), module(), atom(), [term()], timeout()) -> term().
call(Dest, M, F, A, Timeout) ->
    case gen_server:call(Dest, {call, M, F, A}, Timeout) of
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
%%% gen_server callbacks

-record(peer_state, {
    options :: start_options(),
    %% full node name, useful when process it not registered
    node :: atom(),
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
    %% counter (reference) for calls.
    %% it is not possible to use erlang reference, or pid,
    %%  because it changes when node becomes distributed dynamically.
    seq = 0 :: non_neg_integer(),
    %% outstanding calls
    outstanding = #{} :: #{non_neg_integer() => {reference(), pid()}}
}).

-type state() :: #peer_state{}.

-spec init([Name :: atom(), ... ]) -> {ok, state()}.
init([Name, Options]) ->
    process_flag(trap_exit, true), %% need this to ensure terminate/2 is called

    {ListenSocket, Listen} = maybe_listen(Options),
    {Exec, Args} = maybe_remote(command_line(Name, Listen, Options), Listen, Options),

    Env = maps:get(env, Options, []),
    FoundExec = os:find_executable(Exec),
    Port = open_port({spawn_executable, FoundExec}, [{args, Args}, {env, Env}, hide, binary]),

    %% Cannot detach a peer that uses stdio for OOB connection.
    Detached = maps:find(connection, Options) =/= {ok, standard_io},

    %% close port if running detached
    Conn =
        if Detached ->
            %% peer can close the port before we get here,
            %%  which will cause port_close to throw. Catch this
            %%  and ignore.
            catch erlang:port_close(Port),
            receive {'EXIT', Port, _} -> undefined end;
        true ->
            Port
        end,

    %% accept TCP connection if requested
    if ListenSocket =:= undefined ->
            {ok, #peer_state{node = Name, options = Options, connection = Conn}};
        true ->
            prim_inet:async_accept(ListenSocket, ?ACCEPT_TIMEOUT),
            {ok, #peer_state{node = Name, options = Options, listen_socket = ListenSocket}}
    end.

%% not connected: no OOB connection available
handle_call({call, _M, _F, _A}, _From, #peer_state{connection = undefined} = State) ->
    {reply, {error, noconnection}, State};

handle_call({call, M, F, A}, From,
    #peer_state{connection = Port, options = #{connection := standard_io},
        outstanding = Out, seq = Seq} = State) ->
    origin_to_peer(port, Port, {call, Seq, M, F, A}),
    {noreply, State#peer_state{outstanding = Out#{Seq => From}, seq = Seq + 1}};

handle_call({call, M, F, A}, From,
    #peer_state{connection = Socket, outstanding = Out, seq = Seq} = State) ->
    origin_to_peer(tcp, Socket, {call, Seq, M, F, A}),
    {noreply, State#peer_state{outstanding = Out#{Seq => From}, seq = Seq + 1}};

handle_call(wait_boot, _From, #peer_state{peer_state = running} = State) ->
    {reply, ok, State};
handle_call(wait_boot, From, #peer_state{peer_state = booting, wait_boot = WB} = State) ->
    {noreply, State#peer_state{wait_boot = [From | WB]}};
handle_call(wait_boot, _From, #peer_state{peer_state = Other} = State) when Other =:= down; Other =:= shutting_down ->
    {reply, Other, State};

handle_call(get_node, _From, #peer_state{node = Node} = State) ->
    {reply, Node, State};

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

handle_info({inet_async, LSock, _Ref, {error, Reason}},
    #peer_state{listen_socket = LSock} = State) ->
    %% failed to accept a TCP connection
    catch gen_tcp:close(LSock),
    %% stop unconditionally, it is essentially a part of gen_server:init callback
    {stop, {inet_async, Reason}, State#peer_state{connection = undefined, listen_socket = undefined}};

%% booting: peer notifies via Erlang distribution
handle_info({peer_started, Node, Pid}, State) ->
    receive
        {'EXIT', Pid, normal} ->
            true = erlang:monitor_node(Node, true),
            {noreply, boot_complete(State)}
    end;

%% nodedown: no-oob dist-connected peer node is down, stop the server
handle_info({nodedown, Node}, #peer_state{connection = undefined} = State) ->
    maybe_stop({nodedown, Node}, State);

%% port terminated: cannot proceed, stop the server
handle_info({'EXIT', Port, Reason}, #peer_state{connection = Port} = State) ->
    catch erlang:port_close(Port),
    maybe_stop(Reason, State);

handle_info({tcp_closed, Sock}, #peer_state{connection = Sock} = State) ->
    %% TCP connection closed, no i/o port - assume node is stopped
    catch gen_tcp:close(Sock),
    maybe_stop(normal, State).

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
            Node = node_name(Options),
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

%% @doc Internal function, parsing command line, with escaping and quoting support.
parse_args([]) ->
    [];
parse_args(CmdLine) ->
    %% following regex splits command line, preserving quoted arguments, into argv[] list
    Re = "((?:\"[^\"\\\\]*(?:\\\\[\\S\\s][^\"\\\\]*)*\"|'[^'\\\\]*(?:\\\\[\\S\\s][^'\\\\]*)*'|\\/[^\\/\\\\]*(?:\\\\[\\S\\s][^\\/\\\\]*)*\\/[gimy]*(?=\\s|$)|(?:\\\\\\s|\\S))+)(?=\\s|$)",
    {match, Args} = re:run(CmdLine, Re, [{capture, all_but_first, list}, global]),
    %% unquote arguments. It is possible to change regex capture groups to avoid extra processing.
    [unquote(Arg) || [Arg] <- Args].

unquote([Q | Arg]) when Q =:= $\"; Q =:= $\' ->
    case lists:last(Arg) of
        Q -> lists:droplast(Arg);
        _ -> [Q | Arg]
    end;
unquote(Arg) ->
    Arg.

%% Starting new node on a specific host.
start_impl(#{host := _} = Options, Link) ->
    %% if OOB connection is not requested, origin must be alive
    is_map_key(connection, Options) orelse erlang:is_alive() orelse error(not_alive),
    %% ensure that 'name' option is set
    WithName =
        if is_map_key(name, Options) -> Options; %% name is defined
            true ->
                %% compatibility behaviour: use the same name, on a different host
                [Name, _Host] = string:lexemes(atom_to_list(node()), "@"),
                Options#{name => Name}
        end,
    start_it(maybe_rsh(WithName), Link);

%% Starting new node on this host (or through custom rsh).
start_impl(#{name := _Name} = Options, Link) ->
    %% if OOB connection is not requested, origin must be alive
    is_map_key(connection, Options) orelse erlang:is_alive() orelse error(not_alive),
    {Host, Longnames} = origin_hostname(),
    %% set longnames, if it is not explicitly passed
    start_it(Options#{host => Host, longnames => maps:get(longnames, Options, Longnames)}, Link);

%% Neither name not host defined. Peer node is not distributed from start
%%  but may later start distribution dynamically.
start_impl(Options, Link) ->
    %% non-distributed node cannot start without OOB connection
    is_map_key(connection, Options) orelse error(not_alive),
    %% non-distributed peer cannot have local process registered, as
    %%  node name is not known.
    start_it(maps:remove(register, Options), Link).

%% pick start/start_link, register process or not etc.
start_it(Options, Link) ->
    %% create full node name
    {Node, Rego} = node_rego(node_name(Options), Options),
    case start_register(Options, {Node, Rego}, Link) of
        {ok, Pid} when map_get(wait_boot, Options) =:= false ->
            {ok, if Rego =:= false -> Pid; true -> Node end};
        {ok, Pid} ->
            try
                ok = wait_boot(Pid, maps:get(wait_boot, Options, ?WAIT_BOOT_TIMEOUT)),
                {ok, if Rego =:= false -> Pid; true -> Node end}
            catch
                exit:{timeout, _} ->
                    gen_server:stop(Pid),
                    {error, timeout}
            end;
        Error ->
            Error
    end.

%% Execute gen_server:start or start_link, 3 or 4, depending on what
%%  was requested (registered or not, linked or not)
start_register(Options, {Name, false}, Start) ->
    gen_server:Start(?MODULE, [Name, Options], []);
start_register(Options, {Name, Reg}, Start) ->
    gen_server:Start(Reg, ?MODULE, [Name, Options], []).

%% Node name and gen_server registration information
node_rego(Node, #{register := LG}) when LG =:= local; LG =:= global ->
    {Node, {LG, Node}};
node_rego(Node, #{register := {via, Module}})->
    {Node, {via, Module, Node}};
node_rego(Node, #{register := false}) ->
    {Node, false};
node_rego([], _) ->
    %% if peer is not distributed, peer process can't be registered,
    %%  user should explicitly call register('nonode@nohost', Pid) if desired.
    {[], false};
node_rego(Node, _Options) ->
    {Node, {local, Node}}.

node_name(#{name := Name, host := Host}) ->
    list_to_atom(lists:concat([Name, "@", Host]));
node_name(_Options) ->
    [].

%% Origin hostname and short/longnames
-spec origin_hostname() -> {Host :: string(), Longnames :: boolean()}.
origin_hostname() ->
    case erlang:is_alive() of
        true ->
            %% When origin is distributed, use
            %%  origin host and origin long/short names switch. This has an
            %%  interesting effect is peer was started with a host name that does
            %%  not match actual one, "erl -name node@wrong" while inet:gethostname()
            %%  return {ok, "right"}. New peer will also start as "-name peer@wrong".
            [_, Host] = string:lexemes(atom_to_list(node()), "@"),
            {Host, net_kernel:longnames()};
        false ->
            {ok, Host} = inet:gethostname(),
            case inet_db:res_option(domain) of
                [] ->
                    {Host, false};
                Domain ->
                    {Host ++ [$. | Domain], true}
            end
    end.

%% maybe_rsh: compatibility behaviour check: when starting on a different
%%  host, and remote it not set, implicitly add "remote => rsh"
maybe_rsh(#{host := Host} = Options) ->
    %% origin node name is a source of truth (if any)
    case origin_hostname() of
        {Host, Longnames} ->
            longnames(Options, Longnames);
        {_Remote, Longnames} when is_map_key(remote, Options) ->
            longnames(Options, Longnames);
        {_Remote, Longnames} ->
            %% compatibility mode: no remote defined, hostname is different from origin
            longnames(Options#{remote => rsh}, Longnames)
    end.

longnames(Options, _Longnames) when is_map_key(longnames, Options) ->
    %% XXX: no assertion is made for short/long names compatibility,
    %%  to enable peer nodes using different modes
    Options;
longnames(Options, Longnames) ->
    Options#{longnames => Longnames}.

%% @private Two-way link implementation: when two-way link is enabled, peer
%%          process stops when peer node terminates.
maybe_stop(Reason, #peer_state{options = #{link := true}} = State) ->
    {stop, Reason, State#peer_state{peer_state = down, connection = undefined}};
maybe_stop(Reason, #peer_state{peer_state = booting} = State) ->
    {stop, Reason, State#peer_state{peer_state = down, connection = undefined}};
maybe_stop(_Reason, State) ->
    {noreply, State#peer_state{peer_state = down, connection = undefined}}.

%% i/o protocol from origin:
%%  * {io_reply, ...}
%%  * {message, To, Content}
%%  * {call, From, M, F, A}
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
    case node_name(Options) of
        [] ->
            boot_complete(State);
        NodeName ->
            boot_complete(State);
        Unexpected ->
            {stop, {node, Unexpected}, State}
    end.

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
    ByteSize = Size div 8,
    <<BinPart:ByteSize/binary, Before:4>> = Bin,
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
maybe_remote({Exec, Args}, _ListenPort, #{remote := {rsh, Sh, ShArgs}}) ->
    {Sh, ShArgs ++ [Exec | Args]};
maybe_remote({Exec, Args}, ListenPort, #{remote := Remote} = Options) when is_function(Remote, 3) ->
    Remote({Exec, Args}, ListenPort, Options);
maybe_remote(Original, _ListenPort, _Options) ->
    Original.

command_line(Name, Listen, Options) ->
    %% Node name/sname
    NameArg = case {Name, maps:find(longnames, Options)} of
                  {[], _} -> [];
                  {Node, {ok, true}} -> ["-name", atom_to_list(Node)];
                  {Node, {ok, false}} -> ["-sname", atom_to_list(Node)]
              end,
    %% additional command line args
    CmdOpts = maps:get(args, Options, []),
    % start command
    StartCmd =
        case Listen of
            undefined when map_get(connection, Options) =:= standard_io ->
                ["-user", atom_to_list(?MODULE)];
            undefined ->
                Origin = atom_to_list(node()),
                ["-detached", "-noinput", "-s", atom_to_list(?MODULE), "peer_init", Origin, start_relay(),
                    "-master", Origin];
            {Ips, Port} ->
                IpStr = lists:join(",", [inet:ntoa(Ip) || Ip <- Ips]),
                ["-detached", "-noinput", "-user", atom_to_list(?MODULE), "-origin", IpStr, integer_to_list(Port)]
        end,
    %% code path, XXX: remove when included in stdlib
    CodePath = module_path_args(code:which(?MODULE)),
    %% command line: build
    ProgName = progname(Options),
    %% build command line
    {ProgName, NameArg ++ StartCmd ++ CodePath ++ CmdOpts}.

progname(#{progname := Prog}) ->
    Prog;
progname(_Options) ->
    case init:get_argument(progname) of
        {ok, [[Prog]]} ->
            Prog;
        _ ->
            %% if progname is not known, use `erlexec` from the sam ERTS version we're currently running
            %% BINDIR environment variable is already set
            %% EMU variable it also set
            Root = code:root_dir(),
            Erts = filename:join(Root, lists:concat(["erts-", erlang:system_info(version)])),
            BinDir = filename:join(Erts, "bin"),
            filename:join(BinDir, "erlexec")
    end.

module_path_args(cover_compiled) ->
    {file, File} = cover:is_compiled(?MODULE),
    ["-pa", filename:absname(filename:dirname(File))];
module_path_args(Path) when is_list(Path) ->
    ["-pa", filename:absname(filename:dirname(Path))];
module_path_args(_) ->
    [].

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
%% peer node implementation

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
            {Decoded, Used} = binary_to_term(Bytes, [used]),
            %% bytes received from Port, they can never be malformed,
            %%  but they can be somewhat scattered
            Used < byte_size(Bytes) andalso begin
                {_, Remaining} = split_binary(Bytes, Used),
                self() ! {Port, {data, Remaining}} %% re-send back to the queue
            end,
            handle_peer_oob(Kind, Port, Decoded, Refs, Out);
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
            %% this is really not expected to happen, because "do_call"
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
handle_peer_oob(Kind, Port, {call, Seq, M, F, A}, Refs, Out) ->
    CallerRef = do_call(Seq, M, F, A),
    io_server_loop(Kind, Port, Refs#{CallerRef => Seq}, Out#{Seq => CallerRef});
handle_peer_oob(Kind, Port, {message, Dest, Message}, Refs, Out) ->
    Dest ! Message,
    io_server_loop(Kind, Port, Refs, Out).

do_call(Seq, M, F, A) ->
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
