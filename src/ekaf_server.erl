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
-export([start_link/0, send/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {port, pending_request}).

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

send(Topic, Data) ->
    gen_server:call(?SERVER, {request, {kafka_send, Topic, Data}}).

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

handle_call({request, Request}, From, State) ->
    case is_node_alive() of
        true ->
            do_request(From, Request),
            {noreply, State};
        false ->
            case State#state.pending_request of
                undefined ->
                    Port = maybe_start_node(State#state.port),
                    {noreply, State#state{port = Port, pending_request = {From, Request}}};
                _OtherWise ->                   % only one pending request is allowed.
                    {reply, {error, node_down}, State}
            end
    end;
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
            case State#state.pending_request of
                {From, Request} ->
                    do_request(From, Request);
                _ ->
                    ok
            end,
            {noreply, State#state{pending_request = undefined}};
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
maybe_start_node(Port) ->
    case erlang:port_info(Port) of
        undefined ->
            start_java_node();
        _ ->
            Port
    end.

start_java_node() ->
    Java = os:find_executable("java"),
    true = is_list(Java),
    open_port({spawn_executable, Java},
                     [
                      {args, ["-jar", jnode_jar()]},
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
    {Host, Port} = application:get_env(ekaf, ekaf_bootstrap_broker, {"localhost", "9092"}),
    Host ++ ":" ++ integer_to_list(Port).

jnode_jar() ->
    filename:join([code:lib_dir(ekaf),
                   "java_src","target", "JNode-1.0-jar-with-dependencies.jar"]).


do_request(From, {kafka_send, Topic, Data}) ->
    try_produce_sync(From, Topic, Data);
do_request(From, _) ->
    gen_server:reply(From, {error, unknown_request}).

try_produce_sync(From, Topic, Data) ->
    Topic1 = erlang:iolist_to_binary(Topic),
    Data1 =  erlang:iolist_to_binary(Data),
    Req = {self(), From, Topic1, Data1},
    {kafka, get_java_node() } ! Req.

debug(Fmt, Args) ->
     io:format(Fmt, Args).
%% debug(_Fmt, _Args) ->
%%     ok.
