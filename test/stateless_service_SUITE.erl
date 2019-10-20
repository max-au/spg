%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%%     dpg (distributed process groups)
%%% @end
%%% -------------------------------------------------------------------
-module(stateless_service_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases exports
-export([
    local/0, local/1,
    docker/0, docker/1
]).

%% Internal exports: nodes loops, control loops, bootstrap
-export([
    smoke_control/2
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("kernel/include/logger.hrl").

-define (NODE_COUNT, 2).

suite() ->
    [{timetrap, {seconds, 20}}].

init_per_suite(Config) ->
    % build docker image
    %Image = "spg",
    %os:cmd("docker build . -t " ++ Image ++ ":latest"),
    [{node_count, ?NODE_COUNT} | Config].

end_per_suite(Config) ->
    % remove image
    %proplists:get_value(image, Config) =/= undefined andalso
    %    os:cmd("docker rmi " ++ proplists:get_value(image, Config)),
    Config.

all() ->
    [local, docker].

%%--------------------------------------------------------------------
%% Helpers

%% Container start: no container, just a separate BEAM
start_local(SName, ExtraArgs) ->
    DistOptions = "-epmd_module epmd_client -start_epmd false",
    CmdLine = lists:concat([DistOptions, ExtraArgs]),
    % Options
    Options = #{auto_connect => false, connect_all => false, cmd_line => CmdLine,
        connection => {undefined, undefined},
        code_path => [filename:dirname(code:which(epmd_client))]},
    % return Pid of the node
    {ok, Peer} = local_node:start_link(SName, Options),
    Peer.

%%--------------------------------------------------------------------
%% TEST CASES

local() ->
    [{doc, "Cluster discovery: discover node_count pids providing spg service, one on a local host"}].

docker() ->
    [{doc, "Cluster discovery: docker-enabled cluster"}].

%% Stateless service test:
% (a) control node exports 'control' service (in spg scope)
% (b) child nodes export 'smoke' service (in spg scope), totaling to number of child nodes
% (c) child nodes discover 'control' service
% (d) control node discovers child services

local(Config) ->
    NodeCount = ?config(node_count, Config),
    DataPath = ?config(data_dir, Config),

    SysConfig = lists:concat([" -config ", filename:join(DataPath, "sys.config")]),

    % control node
    Control = start_local(control, SysConfig),

    % start N slave containers - either locally or in docker
    SlaveArg = SysConfig ++ " -kernel inet_dist_listen_max 65535",
    Nodes = [start_local(list_to_atom("node" ++ integer_to_list(Seq)), SlaveArg)
        || Seq <- lists:seq(1, NodeCount)],

    % start the app on remote node - for local test only
    ?assertMatch({ok, _SpgPid}, gen_node:rpc(Control, gen_server, start, [{local, spg}, spg, [spg], []])),
    % start the 'control' service on the control node
    ControlPid = gen_node:rpc(Control, erlang, spawn, [fun () -> receive after infinity -> ok end end]),
    % join control service to spg
    ok = gen_node:rpc(Control, spg, join, [control, ControlPid]),

    % start spg scope on the control node
    %  (non-local start that by default, app does it)
    [start_service(Peer) || Peer <- Nodes],

    % run everything through control node, via gen_node RPC
    Result = gen_node:rpc(Control, ?MODULE, smoke_control, [smoke, NodeCount]),
    % here 'Result' is a list of 'spg' processes states, try that
    SpgProcs = [ok || {state, spg, _, _} <- Result],

    % shut everything down
    [gen_node:stop(N) || N <- Nodes ++ [Control]],
    % now verify if all went well
    ?assertEqual(lists:duplicate(NodeCount, ok), SpgProcs).

start_service(Peer) ->
    % start spg app
    {ok, [spg]} = gen_node:rpc(Peer, application, ensure_all_started, [spg]),
    % locate 'control' service
    {ok, _Pid} = gen_node:rpc(Peer, gen_server, start, [stateless_service, {spg, control, 1, 5000}, []]),
    % locate 'service proc'
    ServicePid = gen_node:rpc(Peer, erlang, whereis, [spg]),
    % join service proc
    ok = gen_node:rpc(Peer, spg, join, [smoke, ServicePid]).

%% Actual testcase code here
smoke_control(TestCase, NodeCount) ->
    % wait for 'enough' services (on a control node)
    {ok, Pid} = stateless_service:start_link(spg, TestCase, NodeCount),
    % now wait for all other nodes to have "enough" services
    % they will reply with 'ok'
    Published = spg:get_members(spg, TestCase),
    %
    ?LOG_INFO("Found services: ~s:~s => ~120p", [spg, TestCase, Published]),
    % here we know that 'spg' processes have joined 'smoke' group
    Result = [sys:get_state(SpgService) || SpgService <- Published],
    % time to do the test (benchmark)
    gen_server:stop(Pid),
    % done, finishing, and returning 'ok' multiple times
    Result.

docker(Config) ->
    Image = proplists:get_value(image, Config, "spg"),
    NodeCount = ?config(node_count, Config),

    % control node
    {ok, Control} = docker_node:start_link(control, Image,
        #{auto_connect => false, connect_all => false, host => "control"}),

    % need to wait until 'control' is started and listening, but for how long?
    timer:sleep(500),

    % start N services
    Nodes = [
        begin
            Name = list_to_atom("node" ++ integer_to_list(Seq)),
            {ok, Node} = docker_node:start_link(Name, Image,
                #{auto_connect => false, connect_all => false, link => "control"}),
            Node
        end
        || Seq <- lists:seq(1, NodeCount)],

    %% flush output
    flush_output(Control),

    % now wait until control server gets N services
    ExpectedStr = integer_to_list(NodeCount),
    Reply = wait_for_services(Control, lists:reverse(ExpectedStr)),

    % shut everything down
    [gen_node:stop(N) || N <- Nodes ++ [Control]],
    ?assertEqual(ExpectedStr, Reply).

flush_output(Control) ->
    case gen_node:read(Control, line) of
        empty ->
            ok;
        _ ->
            flush_output(Control)
    end.

wait_for_services(Control, ExpectedRev) ->
    ok = gen_node:write(Control, <<"length(spg:get_members(smoke)).\n">>),
    case get_response(Control, length(ExpectedRev)) of
        ExpectedRev ->
            lists:reverse(ExpectedRev);
        _ ->
            wait_for_services(Control, ExpectedRev)
    end.

get_response(Control, Len) ->
    case gen_node:read(Control, line) of
        empty ->
            timer:sleep(5),
            get_response(Control, Len);
        {value, Line} ->
            Rev = lists:reverse(binary_to_list(Line)),
            lists:sublist(Rev, 1, Len)
    end.
