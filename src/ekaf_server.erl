%%%-------------------------------------------------------------------
%%% @author WangChunye <wcy123@gmail.com>
%%% @copyright (C) 2016, WangChunye
%%% @doc
%%%
%%% @end
%%% Created : 18 Mar 2016 by WangChunye <>
%%%-------------------------------------------------------------------
-module(ekaf_server).

-behaviour(gen_server).

%% API
-export([start_link/0, prepare/0, send/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {port, pending_request = queue:new(), request_for_prepare}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

prepare() ->
    gen_server:call(?SERVER, prepare).

send(Topic, Data) ->
    gen_server:call(?SERVER, {request, {kafka_send, Topic, Data}}).
recv() ->
    Ref = erlang:make_ref(),
    {kafka_consumer, get_java_node() } ! {self(), Ref},
    receive
        {Ref, Binary} ->
            Binary
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    debug("start to monirot ~p~n", [self()]),
    process_flag(trap_exit, true),
    ok = net_kernel:monitor_nodes(true, [{node_type, all}]),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_call(prepare, From, State) ->
    handle_prepare(From,State);
handle_call({request, Request}, From, State) ->
    handle_request(From, Request, State);
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({_Port, {data, Data}}, State) ->
    debug("JAVANODE: ~s~n", [Data]),
    {noreply, State};
handle_info({_Port, {exit_status,Status}}, State) ->
    debug("JAVANODE: exit = ~p~n", [Status]),
    {stop, nodedown, State};
handle_info({nodedown, Node, _Info}, State) ->
    debug("node down ~p~n", [Node]),
    case Node == get_java_node() of
        true ->
            {stop, nodedown, State};
        false ->
            {noreply, State}
    end;
handle_info({nodeup, Node, _Info}, State) ->
    debug("node up ~p~n", [Node]),
    case Node == get_java_node() of
        true ->
            State0 = maybe_reply_prepare_request(State),
            State1 = maybe_handle_pending_request(State0),
            {noreply, State1};
        false ->
            {noreply, State}
    end;
handle_info({java, From, Reply}, State) ->
    gen_server:reply(From, Reply),
    {noreply, State};
handle_info(Info, State) ->
    debug("UNKNOWN INFO: Info = ~p~n", [Info]),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, _State) ->
    debug("terminate with reason ~p~n", [Reason]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
handle_prepare(From,State) ->
    case is_node_alive() of
        true ->
            {reply, ok, State};
        false ->
            do_prepare(From, State)
    end.

do_prepare(From, State) ->
    TargetNode = get_java_node(),
    %% trigger the node up event
    net_kernel:hidden_connect_node(TargetNode),
    net_adm:ping(TargetNode),
    case is_node_alive() of
        true ->
            {reply, ok, State#state{request_for_prepare = undefined}};
        false ->
            State0 = maybe_start_node(State),
            {noreply,State0#state{request_for_prepare = From}}
    end.

handle_request(From, Request, State) ->
    case is_node_alive() of
        true ->
            do_request(From, Request),
            {noreply, State};
        false ->
            case is_overflow(State) of
                true ->
                    { reply, {error, overflow}, State};
                false ->
                    State0 = maybe_start_node(State),
                    State1 = push_request(From, Request, State0),
                    { noreply, State1}
            end
    end.

is_overflow(State) ->
    queue:len(State#state.pending_request) > 100.


maybe_reply_prepare_request(State) ->
    case State#state.request_for_prepare of
        undefined ->
            ok;
        From ->
            gen_server:reply(From, ok)
    end,
    State#state{ request_for_prepare = undefined }.

maybe_handle_pending_request(State) ->
    lists:foreach(
      fun({From, Request}) ->
              do_request(From, Request)
      end,
      queue:to_list(State#state.pending_request)),
    State#state{ pending_request = queue:new() }.

push_request(From, Request, State) ->
    Q = State#state.pending_request,
    State#state{pending_request = queue:in({From, Request}, Q)}.


maybe_start_node(State) ->
    %% Port = State#state.port,
    %% NewPort =
    %%     case erlang:port_info(Port) of
    %%         undefined ->
    %%             %% start_java_node();
    %%             ok;
    %%         _ ->
    %%             Port
    %%     end,
    net_adm:ping(get_java_node()),
    State#state{ port = ok}.

start_java_node() ->
    Java = os:find_executable("java"),
    true = is_list(Java),
    open_port({spawn_executable, Java},
                     [
                      {args, ["-jar", jnode_jar(), "-classpath", "."]},
                      binary,
                      exit_status,
                      {env, [
                             {"FIRST_NODE", atom_to_list(node())},
                             {"BROKER_LIST", build_broker_list()},
                             {"ERLANG_COOKIE", atom_to_list(erlang:get_cookie())}
                            ]}
                     ]).

get_java_node() ->
    list_to_atom("java" ++ "@" ++ inet_db:gethostname()).

is_node_alive() ->
    lists:member(get_java_node(), nodes(hidden)).

build_broker_list() ->
    {Host, Port} = application:get_env(ekaf, ekaf_bootstrap_broker, {"localhost", 9092}),
    Host ++ ":" ++ integer_to_list(Port).

jnode_jar() ->
    filename:join([code:lib_dir(ekaf),
                   "java_src","target", "JNode-1.0-jar-with-dependencies.jar"]).


do_request(From, {kafka_send, Topic, Data}) ->
    try_produce_sync(From, Topic, Data);
do_request(From, _) ->
    gen_server:reply(From, {error, unknown_request}).

try_produce_sync({From, Tag}, Topic, Data) ->
    Topic1 = erlang:iolist_to_binary(Topic),
    Data1 =  erlang:iolist_to_binary(Data),
    Req = {produce, From, Tag, Topic1, Data1},
    {kafka, get_java_node() } ! Req.

debug(Fmt, Args) ->
     io:format(Fmt, Args).
%% debug(_Fmt, _Args) ->
%%     ok.
