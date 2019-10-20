-module(spg_dist).

-export([listen/1, accept/1, accept_connection/5,
    setup/5, close/1, select/1]).

%% Optional
-export([setopts/2, getopts/2]).

%% internal exports

-include_lib("kernel/include/net_address.hrl").

-include_lib("kernel/include/dist.hrl").
-include_lib("kernel/include/dist_util.hrl").

%% ------------------------------------------------------------
%%  Select this protocol based on node name
%%  select(Node) => Bool
%% ------------------------------------------------------------

select(_Node) ->
    true.

%% ------------------------------------------------------------
%% Create the listen socket, i.e. the port that this erlang
%% node is accessible through.
%% ------------------------------------------------------------

listen(Name) ->
    %epmd_client:log_append("~s: listen ~p", [?MODULE, Name]),
    inet_tcp_dist:listen(Name).

accept(Listen) ->
    %epmd_client:log_append("~s: accept ~p", [?MODULE, Listen]),
    inet_tcp_dist:accept(Listen).

accept_connection(AcceptPid, DistCtrl, MyNode, Allowed, SetupTime) ->
    inet_tcp_dist:accept_connection(AcceptPid, DistCtrl, MyNode, Allowed, SetupTime).

setup(Node, Type, MyNode, LongOrShortNames,SetupTime) ->
    epmd_client:log_append("~s: connect to ~s type ~s (~s)", [?MODULE, Node, Type, LongOrShortNames]),
    inet_tcp_dist:setup(Node, Type, MyNode, LongOrShortNames,SetupTime).

close(Listen) ->
    inet_tcp_dist:close(Listen).

setopts(S, Opts) ->
    inet_tcp_dist:setopts(S, Opts).

getopts(S, Opts) ->
    inet_tcp_dist:getopts(S, Opts).
