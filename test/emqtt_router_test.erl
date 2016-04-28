%%
%%
%%

-module(emqtt_router_test).

-include_lib("eunit/include/eunit.hrl").

-define(setup(F), {setup, fun start/0, fun stop/1, F}).

start_stop_test_() ->
    [{"The server can be started, stopped and has a registered name", 
            ?setup(fun is_registered/1) }].

%%
%% Starting and stopping
%%

is_registered(Pid) ->
    [?_assert(erlang:is_process_alive(Pid)),
     ?_assertEqual(Pid, whereis(emqtt_router))].

%%
%% Topics tests
%%

topics_test_() ->
    [ 
        {"No topics", ?setup(fun no_topics_found/1)},
        {"Single subscribe test", ?setup(fun single_subscribe/1)},
        {"Two subscribes test", ?setup(fun two_subscribes/1)},
        {"Wildcard subscribe test", ?setup(fun wildcard_subscribe/1)}
    ].

no_topics_found(_) ->
    Topics = emqtt_router:topics(), 
    [?_assertEqual([], Topics)].

single_subscribe(_) ->
    ok = emqtt_router:subscribe({"/test/this", 0}, self()),
    Topics = emqtt_router:topics(),
    Match = emqtt_router:match(<<"/test/this">>),
    [?_assertEqual([<<"/test/this">>], Topics),
        ?_assertEqual([{topic, <<"/test/this">>, node()}], Match) 
    ].

two_subscribes(_) ->
    ok = emqtt_router:subscribe({"/test/one", 0}, self()),
    ok = emqtt_router:subscribe({"/test/two", 0}, self()),
    Topics = emqtt_router:topics(),

    Match1 = emqtt_router:match(<<"/test/one">>),
    Match2 = emqtt_router:match(<<"/test/two">>),
    Match3 = emqtt_router:match(<<"/test">>),

    [?_assertEqual([<<"/test/one">>, <<"/test/two">>], lists:sort(Topics)),
        ?_assertEqual([{topic, <<"/test/one">>, node()}], Match1),
        ?_assertEqual([{topic, <<"/test/two">>, node()}], Match2),
        ?_assertEqual([], Match3) 
    ].

wildcard_subscribe(_) ->
    ok = emqtt_router:subscribe({"/#", 0}, self()),
    Topics = emqtt_router:topics(),
    Match1 = emqtt_router:match(<<"/test/this">>),
    Match2 = emqtt_router:match(<<"/test/this/also">>),
    [?_assertEqual([<<"/#">>], lists:sort(Topics)),
        ?_assertEqual([{topic, <<"/#">>, node()}], Match1),
        ?_assertEqual([{topic, <<"/#">>, node()}], Match1)
    ].

%%
%% Setup and teardown
%%

start() ->
    error_logger:tty(false),
    application:start(mnesia),
    {ok, _} = application:ensure_all_started(emqtt),
    error_logger:tty(true),
    whereis(emqtt_router).

stop(Pid) ->
    error_logger:tty(false),
    application:stop(emqtt),
    application:stop(mnesia),
    error_logger:tty(true).
