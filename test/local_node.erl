%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%%     Peer node implementation, capable of starting and joining nodes
%%%      to distribution cluster.
%%% @end
%%% -------------------------------------------------------------------
-module(local_node).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/2
]).

%% gen_node callbacks
-export([
    terminate_node/4,
    command_line/3
]).

-behaviour(gen_node).

%% Default host address: localhost, ipv4.
-define (LOCALHOST, {127, 0, 0, 1}).

-type stop_method() :: gen_node:stop_method() | port.

-export_type([
    stop_method/0
]).

-define (HALT_TIMEOUT, 2000).

-define (DEFAULT_STOP_METHODS, [stop, close, port, term, kill]).

%% @doc starts peer Erlang node on this host.
-spec start_link(Node :: atom(), Options :: gen_node:start_options()) -> {ok, pid()} | {error, Reason :: term()}.
start_link(Name, Options) ->
    StopMethods = maps:get(stop_methods, Options, ?DEFAULT_STOP_METHODS),
    gen_node:start_link(?MODULE, Name, Options#{stop_methods => StopMethods}).

%%--------------------------------------------------------------------
%%% gen_node callbacks

-spec terminate_node(node(), port(), integer(), atom()) -> ok | skip | default | {port(), integer()}.
% port is not connected
terminate_node(_, undefined, OsPid, port) ->
    {undefined, OsPid};

% port is connected
terminate_node(_, Port, OsPid, port) ->
    try
        % send 'halt' to that node stdin
        erlang:port_command(Port, <<"halt().\n">>),
        receive
            {Port, {exit_status, _ErrorLevel}} ->
                % port being closed is good enough
                ok
        after ?HALT_TIMEOUT ->
            {undefined, OsPid}
        end
    catch
        % port was closed before we got here
        error:badarg ->
            {undefined, OsPid}
    after
        (catch erlang:port_close(Port))
    end;

terminate_node(_, _Port, _OsPid, _Method) ->
    default.

command_line(Node, ListenPort, Options) ->
    % add code paths
    CodeDirs = [filename:dirname(code:which(?MODULE)) | maps:get(code_path, Options, [])],
    CodePath = lists:concat([" -pa " ++ Path || Path <- CodeDirs]),
    % cookie: 'default' is what we have for node()
    Cookie =
        case maps:find(cookie, Options) of
            {ok, CookieString} when is_list(CookieString) ->
                lists:concat([" -setcookie ", CookieString]);
            {ok, undefined} ->
                "";
            error ->
                lists:concat([" -setcookie ", erlang:get_cookie()])
        end,
    %
    [ShortNode | ShortHost] = string:split(atom_to_list(Node), "@"),
    % crash dump file
    CrashDumpPath =
        case maps:find(crash_dump_location, Options) of
            {ok, default} ->
                " -env ERL_CRASH_DUMP \"" ++ filename:join(
                    test_server_sup:crash_dump_dir(), lists:concat(["erl_crash_dump.", ShortNode])) ++ "\"";
            {ok, Loc} when is_list(Loc) ->
                " -env ERL_CRASH_DUMP \"" ++ Loc ++ "\"";
            error ->
                ""
        end,
    % long/short names
    {NameArg, FullNode} =
        case maps:get(longnames, Options, net_kernel:longnames()) of
            true when ShortHost =:= "" ->
                % longnames, host not specified
                Host = inet_db:gethostname(),
                Domain = inet_db:res_option(domain),
                {lists:concat([" -name ", Node]), lists:concat([Node, "@", Host, ".", Domain])};
            true ->
                {lists:concat([" -name ", Node]), lists:concat([Node, "@", ShortHost])};
            false when ShortHost =:= "" ->
                % shortnames, host not specified
                Host = inet_db:gethostname(),
                {lists:concat([" -sname ", Node]), lists:concat([Node, "@", Host])};
            false ->
                {lists:concat([" -sname ", Node]), lists:concat([Node, "@", ShortHost])}
        end,
    % detached
    Detached = case maps:find(detached, Options) of
                   {ok, true} ->
                       " -detached";
                   {ok, false} ->
                       "";
                   error ->
                       ""
               end,
    % dist_auto_connect
    ConnectAllOpts =
        case maps:get(connect_all, Options, true) of
            false ->
                " -connect_all false -kernel dist_auto_connect never";
            true ->
                ""
        end,
    % additional command line args
    CmdOpts = case maps:get(cmd_line, Options, "") of "" -> ""; Args -> [$  | Args] end,
    % start command
    StartCmd =
        case maps:find(start, Options) of
            {ok, StartCmd1} ->
                [$  | StartCmd1];
            error ->
                lists:concat([" -s ", gen_node, " slave_loop ", integer_to_list(ListenPort)])
        end,
    % command line: build
    CmdLine = lists:concat(["erl -noshell", StartCmd, NameArg, Detached,
        CrashDumpPath, Cookie, CodePath, ConnectAllOpts, CmdOpts]),
    {list_to_atom(FullNode), CmdLine}.

%%--------------------------------------------------------------------
%% Internal implementation
