%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%%     Peer node implementation, capable of starting and joining nodes
%%%      to distribution cluster.
%%% @end
%%% -------------------------------------------------------------------
-module(docker_node).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/3
]).

%% gen_node callbacks
-export([
    terminate_node/4,
    command_line/3
]).

-behaviour(gen_node).

% Halt timeout: Docker may take longer to exit
-define (HALT_TIMEOUT, 5000).

%% Default host name
-define (HOSTNAME, "host.docker.internal").

-type stop_method() :: port | stop.

-export_type([
    stop_method/0
]).

-define (DEFAULT_STOP_METHODS, [stop, port, term]).

%% @doc starts peer Erlang node inside Docker container (image must already exist!)
-spec start_link(Node :: atom(), Image :: string(), Options :: gen_node:start_options()) -> {ok, pid()} | {error, Reason :: term()}.
start_link(Name, Image, Options) ->
    StopMethods = maps:get(stop_methods, Options, ?DEFAULT_STOP_METHODS),
    gen_node:start_link(?MODULE, Name, Options#{listen_port => disabled, image => Image, stop_methods => StopMethods}).

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

terminate_node(_, _Port, _OsPid, _Method)  ->
    default.

command_line(Node, ListenPort, #{image := Image} = Options) ->
    % -h control.docker --name control.docker
    {FullNode, HostName} =
        case maps:find(host, Options) of
            error ->
                {lists:concat([Node, "@", "to_be_discovered"]), ""};
            {ok, Host} ->
                {lists:concat([Node, "@", Host]), lists:concat([" -h ", Host, " --name ", Host])}
        end,
    ExposePort = case ListenPort of
                     undefined ->
                         "";
                     {_Host, Port} ->
                         " -p " ++ integer_to_list(Port) ++ ":" ++ integer_to_list(Port)
                 end,
    Link =
        case maps:find(link, Options) of
            error ->
                "";
            {ok, LinkTo} ->
                lists:concat([" --link ", LinkTo])
        end,
    % -d, --detach=true|false : detached
    % -i, --interactive=true|false Keep STDIN open even if not attached. The default is false.
    Detached = case maps:get(detached, Options, false) of true -> " -d "; false -> " -i " end,
    % --rm true|false Automatically remove the container when it exits. The default is false.
    CmdLine = lists:concat(["docker run --rm", ExposePort, HostName, Link, Detached, Image]),
    {FullNode, CmdLine}.

%%--------------------------------------------------------------------
%% Internal implementation
