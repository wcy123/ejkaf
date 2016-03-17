%%%-------------------------------------------------------------------
%%% @author WangChunye <>
%%% @copyright (C) 2016, WangChunye
%%% @doc
%%%
%%% @end
%%% Created : 17 Mar 2016 by WangChunye <>
%%%-------------------------------------------------------------------
-module(ekaf_server).

-behaviour(gen_fsm).

%% API
-export([start_link/0]).

%% gen_fsm callbacks
-export([init/1,
         node_down/2, node_down/3,
         node_up/2, node_up/3,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {port, pending_request}).

%%%===================================================================
%%% API
%%%===================================================================
send(Topic, Data) ->
    gen_fsm:sync_send_event(?SERVER, {request, {kafka_send, Topic, Data}}).

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    debug("start to monirot ~p~n", [self()]),
    process_flag(trap_exit, true),
    ok = net_kernel:monitor_nodes(true, [{node_type, all}]),
    {ok, node_down, #state{}, 0}.

node_down({request, Request}, From, State) ->
    Port = maybe_start_node(State#state.port),
    case State#state.pending_request of
        undefined ->
            {next_state, node_down, State#state{port = Port, pending_request = {From, Request}}};
        _ ->
            {reply, {error, node_down}, node_down, State}
    end;
node_down(Event, _From, State) ->
    debug("UNKNOWN EVENT in state node_down: Event = ~p~n", [Event]),
    {next_state, node_down, State}.

node_down(Event, State) ->
    debug("UNKNOWN ASYNC EVENT in state node_down: Event = ~p~n", [Event]),
    {next_state, node_down, State}.

node_up({request, Request}, From, State) ->
    case is_node_alive() of
        true ->
            Reply = do_request(Request),
            {reply, Reply, node_up, State};
        false ->
            Port = maybe_start_node(State#state.port),
            {next_state, node_down, State#state{port = Port, pending_request = {From, Request}}}
    end;
node_up(Event, _From, State) ->
    debug("UNKNOWN EVENT in state node_up: Event = ~p~n", [Event]),
    {next_state, node_up, State}.

node_up(Event, State) ->
    debug("UNKNOWN ASYNC EVENT in state node_up: Event = ~p~n", [Event]),
    {next_state, node_up, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info({_Port, {data, Data}}, StateName, State) ->
    debug("JAVANODE(~p): ~s~n", [StateName, Data]),
    {next_state, StateName, State};
handle_info({_Port, {exit_status,Status}}, StateName, State) ->
    debug("JAVANODE(~p): exit = ~p~n", [StateName, Status]),
    {next_state, StateName, State#state{port = undefined}};
handle_info({nodedown, Node, _Info}, StateName, State) ->
    debug("node down ~p~n", [Node]),
    case Node == get_java_node() of
        true ->
            {next_state, node_down, State};
        false ->
            {next_state, StateName, State}
    end;
handle_info({nodeup, Node, _Info}, StateName, State) ->
    debug("node up ~p~n", [Node]),
    case Node == get_java_node() of
        true ->
            case State#state.pending_request of
                {From, Request} ->
                    Reply = do_request(Request),
                    gen_fsm:reply(From, Reply);
                _ ->
                    ok
            end,
            {next_state, node_up, State#state{pending_request = undefined}};
        false ->
            {next_state, StateName, State}
    end;
handle_info(Info, StateName, State) ->
    debug("UNKNOWN INFO: Info = ~p, StateName = ~p~n", [Info, StateName]),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, StateName, State) ->
    debug("SERVER EXIST: Reason = ~p, StateName = ~p~n",[Reason, StateName]),
    erlang:port_close(State#state.port),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

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
                             {"BROKER_LIST", build_broker_list()}
                            ]}
                     ]).

get_java_node() ->
    list_to_atom("java" ++ "@" ++ inet_db:gethostname()).

is_node_alive() ->
    lists:member(get_java_node(), nodes(hidden)).

build_cmd() ->
    "java -jar " ++ jnode_jar() ++ " " ++ build_args().

build_args() ->
    build_broker_list().

build_broker_list() ->
    {Host, Port} = application:get_env(ekaf, ekaf_bootstrap_broker, {"localhost", "9092"}),
    Host ++ ":" ++ Port.

jnode_jar() ->
    filename:join([code:lib_dir(ekaf),
                   "java_src","target", "JNode-1.0-jar-with-dependencies.jar"]).


do_request({kafka_send, Topic, Data}) ->
    try_produce_sync(Topic, Data);
do_request(_) ->
    {error, unknown_request}.

try_produce_sync(Topic, Data) ->
    Topic1 = erlang:iolist_to_binary(Topic),
    Data1 =  erlang:iolist_to_binary(Data),
    Ref = make_ref(),
    Req = {self(), Ref, Topic1, Data1},
    {kafka, get_java_node() } ! Req,
    receive
        {Ref, Ret} ->
            Ret
    after 5000 ->
            {error, timeout}
    end.

%% debug(Fmt, Args) ->
%%     io:format(Fmt, Args).
debug(_Fmt, _Args) ->
    ok.
