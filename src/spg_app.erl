%%%-------------------------------------------------------------------
%%% @author Maxim Fedorov <maximfca@gmail.com>
%%% @doc
%%% Application behaviour.
%%% @end

-module(spg_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%%-------------------------------------------------------------------
%% API

start(_StartType, _StartArgs) ->
    spg_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.
