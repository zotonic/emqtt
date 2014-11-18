
-module(emqtt_router).

-include("emqtt.hrl").
-include("emqtt_internal.hrl").

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(gen_server).

-export([
    start_link/0,
    topics/0,
    subscribe/2, 
    unsubscribe/2,
    publish/1,
    publish/2,
    route/2,
    match/1,
    down/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(NODE_TABLE, emqtt_router_node).
-define(TRIE_TABLE, emqtt_router_trie).
-define(TOPIC_TABLE, emqtt_router_topic).
-define(SUBSCRIBER_TABLE, emqtt_router_subscriber).

-record(state, {}).

%%
%% Api
%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


% @doc
topics() ->
    [Name || #topic{name=Name} <- ets:tab2list(?TOPIC_TABLE)].


% @doc
subscribe({Topic, Qos}, Client) when is_pid(Client) ->
    gen_server:call(?MODULE, {subscribe, {to_binary(Topic), Qos}, Client});
subscribe(Topic, Client) ->
    subscribe({Topic, ?QOS_0}, Client).

% @doc
unsubscribe(Topic, Client) when is_pid(Client) ->
    gen_server:call(?MODULE, {unsubscribe, to_binary(Topic), Client}).

publish(Msg=#mqtt_msg{topic=Topic}) ->
        publish(Topic, Msg).

%publish to cluster node.
publish(Topic, Msg) when is_record(Msg, mqtt_msg) ->
    lists:foreach(fun(#topic{name=Name, node=Node}) ->
                case Node == node() of
                    true -> 
                        route(Name, Msg);
                    false -> 
                        rpc:call(Node, ?MODULE, route, [Name, Msg])
                end
        end, match(Topic)).

%route locally, should only be called by publish
route(Topic, Msg) when is_binary(Topic) ->
    [Client ! {route, Msg#mqtt_msg{qos=Qos}} || #subscriber{qos=Qos, client=Client} <- ets:lookup(?SUBSCRIBER_TABLE, Topic)].

% @doc
match(Topic) ->
    TrieNodes = trie_match(emqtt_topic:words(to_binary(Topic))),
    Topics = [ets:lookup(?TOPIC_TABLE, Name) || #trie_node{topic=Name} <- TrieNodes, 
        Name =/= undefined],
    lists:flatten(Topics).

% @doc
down(Client) when is_pid(Client) ->
    gen_server:cast(?MODULE, {down, Client}).
    

%%
%% Gen-server callbacks
%%

init([]) ->
    ets:new(?NODE_TABLE, [protected, named_table, {read_concurrency, true}, {keypos, 2}]),
    ets:new(?TRIE_TABLE, [protected, named_table, {read_concurrency, true}, {keypos, 2}]),
    ets:new(?TOPIC_TABLE, [protected, named_table, bag, {read_concurrency, true}, {keypos, 2}]),
    ets:new(?SUBSCRIBER_TABLE, [protected, named_table, bag, {read_concurrency, true}, {keypos, 2}]),

    lager:info("~p is started.", [?MODULE]),
    {ok, #state{}}.

handle_call({subscribe, {Topic, Qos}, Client}, _From, State) ->
    trie_add(Topic),
    emqtt_client_monitor:mon(Client),
    ets:insert(?SUBSCRIBER_TABLE, #subscriber{topic=Topic, qos=Qos, client=Client}),
    emqtt_retained:send(Topic, Client),
    {reply, ok, State};

handle_call({unsubscribe, Topic, Client}, _From, State) ->
    ets:match_delete(?SUBSCRIBER_TABLE, #subscriber{topic=Topic, client=Client, _='_'}),
    try_remove_topic(Topic),
    {reply, ok, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    {stop, {badreq,Req}, State}.

handle_cast({down, Client}, State) ->
        case ets:match_object(?SUBSCRIBER_TABLE, #subscriber{client=Client, _='_'}) of
            [] -> ignore;
            Subs ->
                [ets:delete_object(?SUBSCRIBER_TABLE, Sub) || Sub <- Subs],
                [try_remove_topic(Topic) || #subscriber{topic=Topic} <- Subs]
        end,
        {noreply, State};
handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info(Info, State) ->
    {stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%
%% Helpers
%%

try_remove_topic(Name) ->
    case ets:member(?SUBSCRIBER_TABLE, Name) of
        false ->
            Topic = emqtt_topic:new(Name),
            ets:delete_object(?TOPIC_TABLE, Topic),
            case ets:lookup(?TOPIC_TABLE, Name) of
                [] ->
                    trie_delete(Name);
                _ ->
                    ignore
            end;
        true -> 
            ok
        end.

trie_add(Topic) ->
    ets:insert(?TOPIC_TABLE, emqtt_topic:new(Topic)),
    case ets:lookup(?NODE_TABLE, Topic) of
        [#trie_node{topic=undefined}=Node] ->
            ets:insert(?NODE_TABLE, Node#trie_node{topic=Topic});
        [#trie_node{topic=Topic}] ->
            ok;
        [] ->
            Triples = emqtt_topic:triples(Topic),
            [trie_add_path(Triple) || Triple <- Triples],
            TrieNode = #trie_node{node_id=Topic, topic=Topic},
            ets:insert(?NODE_TABLE, TrieNode)
    end.

trie_delete(Topic) ->
    case ets:lookup(?NODE_TABLE, Topic) of
        [#trie_node{edge_count=0}] ->
            ets:delete(?NODE_TABLE, Topic),
            trie_delete_path(lists:reverse(emqtt_topic:triples(Topic)));
        [TrieNode] ->
            ets:insert(?NODE_TABLE, TrieNode#trie_node{topic=Topic});
        [] -> 
            ignore
    end.


trie_add_path({Node, Word, Child}) ->
    Edge = #trie_edge{node_id=Node, word=Word},
    case ets:lookup(?NODE_TABLE, Node) of
        [#trie_node{edge_count=_Count}] ->
            case ets:lookup(?TRIE_TABLE, Edge) of
                [] ->
                    Trie = #trie{edge=Edge, node_id=Child},
                    ets:update_counter(?NODE_TABLE, Node, {#trie_node.edge_count, +1}),
                    ets:insert(?TRIE_TABLE, Trie);
                [_] ->
                    ok
            end;
        [] ->
            TrieNode = #trie_node{node_id=Node, edge_count=1},
            Trie = #trie{edge=Edge, node_id=Child},
            ets:insert(?NODE_TABLE, TrieNode),
            ets:insert(?TRIE_TABLE, Trie)
    end.

trie_delete_path([]) ->
    ok;
trie_delete_path([{NodeId, Word, _} | RestPath]) ->
    Edge = #trie_edge{node_id=NodeId, word=Word},
    ets:delete(?TRIE_TABLE, Edge),

    case ets:lookup(?NODE_TABLE, NodeId) of
        [#trie_node{edge_count=1, topic=undefined}] ->
            ets:delete(?NODE_TABLE, NodeId),
            trie_delete_path(RestPath);
        [#trie_node{edge_count=1, topic=Topic}] ->
            ets:update_counter(?NODE_TABLE, NodeId, {#trie_node.edge_count, -1}),
            case ets:lookup(?TOPIC_TABLE, Topic) of
                [] ->
                    %% This topic is gone too.
                    trie_delete(Topic);
                _ ->
                    ok
            end;
        [#trie_node{edge_count=Count}] when Count >= 1 ->
            ets:update_counter(?NODE_TABLE, NodeId, {#trie_node.edge_count, -1});

        [] ->
            throw({notfound, NodeId})
    end.


trie_match(Words) ->
    trie_match(root, Words, []).

trie_match(NodeId, [], ResAcc) ->
    Found = ets:lookup(?NODE_TABLE, NodeId),
    Found ++ 'trie_match_#'(NodeId, ResAcc);

trie_match(NodeId, [W|Words], ResAcc) ->
    lists:foldl(fun(WArg, Acc) ->
                case ets:lookup(?TRIE_TABLE, #trie_edge{node_id=NodeId, word=WArg}) of
                    [#trie{node_id=ChildId}] -> 
                        trie_match(ChildId, Words, Acc);
                    [] -> 
                        Acc
                end
        end, 'trie_match_#'(NodeId, ResAcc), [W, $+]).

'trie_match_#'(NodeId, ResAcc) ->
    case ets:lookup(?TRIE_TABLE, #trie_edge{node_id=NodeId, word=$#}) of
        [#trie{node_id=ChildId}] ->
            Found = ets:lookup(?NODE_TABLE, ChildId),
            Found ++ ResAcc;
        [] ->
            ResAcc
    end.

to_binary(L) when is_list(L) ->
    unicode:characters_to_binary(L);
to_binary(B) when is_binary(B) ->
    B.

%%
%% Tests
%%

-ifdef(TEST).

-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).

is_started_test_() ->
    ?setup(fun(Pid) ->
                [?_assertEqual(Pid, whereis(?MODULE))]
        end).

subscribe_unsubscribe_test_() ->
    ?setup(fun(_Pid) ->
                subscribe(<<"a/b/c">>, self()),
                subscribe(<<"a/#">>, self()),

                T1 = topics(),

                M1 = match(<<"a/c">>),
                M2 = match(<<"a/b/c">>),
                M3 = match(<<"a/b/c/d">>),
                M4 = match(<<"x/b/c/d">>),

                unsubscribe(<<"a/#">>, self()),
                T2 = topics(),

                M5 = match(<<"a/c">>),
                M6 = match(<<"a/b/c">>),

                subscribe(<<"a/+/c">>, self()),

                M7 = match(<<"a/x/c">>),
                M8 = match(<<"a/b/c">>),

                [ ?_assertEqual([#topic{name= <<"a/#">>, node=node()}], M1),
                  ?_assertEqual([#topic{name= <<"a/#">>, node=node()},
                                 #topic{name= <<"a/b/c">>, node=node()}], lists:sort(M2)),
                  ?_assertEqual([#topic{name= <<"a/#">>, node=node()}], lists:sort(M3)),
                  ?_assertEqual([], M4),
                  ?_assertEqual([], M5),
                  ?_assertEqual([#topic{name= <<"a/b/c">>, node=node()}], M6),
                  ?_assertEqual([#topic{name= <<"a/+/c">>, node=node()}], M7),

                  ?_assertEqual([#topic{name= <<"a/+/c">>, node=node()},
                                 #topic{name= <<"a/b/c">>, node=node()} ], lists:sort(M8)),

                  ?_assertEqual([<<"a/#">>, <<"a/b/c">>], lists:sort(T1)),
                  ?_assertEqual([<<"a/b/c">>], T2)
              ]
        end).

random_sub_match_test() ->
    Pid = setup(),
    try
        ?assertEqual(true, proper:quickcheck(subscribe_props(), [{to_file, user}]))
    after
        teardown(Pid)
    end,
    ok.

subscribe_props() ->
    ?FORALL(
       IntTopics,
       list(list(int())),
       begin 
           Topics = [to_topic(IntTopic, <<>>) || IntTopic <- IntTopics],


           %% Test subscribing to the topics one by one.
           subscribe_multi_props(Topics, []),

           %% Now, unsubscribe from all topics. 
           %%
           %% Because unsubscribe removes multi regestrations we have to 
           %% setify the Topics before running the test.
           unsubscribe_multi_props(setify(Topics)),

           %% Now there should be no more subscriptions
           ?assertEqual([], topics()),

           %% And we should have no more nodes and edges dangling
           ?assertEqual([], ets:tab2list(?TRIE_TABLE)),
           ?assertEqual([], ets:tab2list(?NODE_TABLE)),

           true
       end).

subscribe_multi_props([], _) ->
    ok;
subscribe_multi_props([Topic|Rest], Registered) ->
    subscribe(Topic, self()),
    NowRegistered = setify([Topic | Registered]),
    ?assertEqual(NowRegistered, setify(topics())),
    subscribe_multi_props(Rest, NowRegistered).

unsubscribe_multi_props([]) ->
    ok;
unsubscribe_multi_props([Topic|Rest]) ->
    unsubscribe(Topic, self()),
    ?assertEqual(setify(topics()), lists:sort(Rest)),
    unsubscribe_multi_props(Rest).

setify(L) ->
    S = sets:from_list(L),
    L1 = sets:to_list(S),
    lists:sort(L1).

to_topic([], <<>>) ->
    <<"#">>;
to_topic([], Topic) ->
    Topic;
to_topic([H|T], Topic) ->
    B = list_to_binary(integer_to_list(H)),
    to_topic(T, <<Topic/binary, $/, B/binary>>).


setup() ->
    {ok, Pid} = start_link(),
    Pid.

teardown(_) ->
    gen_server:call(?MODULE, stop).

-endif.

