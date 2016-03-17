-module(ekaf).
-export([prepare/1, produce_sync/2]).

prepare(_Topic) ->
    maybe_start_java_node(),
    ok.

produce_sync(Topic, Data) ->
    produce_sync(Topic, Data, 5000).

produce_sync(Topic, Data, Timeout) ->
    case is_node_alive() orelse net_kernel:hidden_connect(get_java_node()) of
        false ->
            {error, nodedown};
        true ->
            try_produce_sync(Topic, Data, Timeout)
    end.

try_produce_sync(Topic, Data, Timeout) ->
    Topic1 = erlang:iolist_to_binary(Topic),
    Data1 =  erlang:iolist_to_binary(Data),
    Ref = make_ref(),
    Req = {self(), Ref, Topic1, Data1},
    {kafka, get_java_node() } ! Req,
    receive
        {Ref, Ret} ->
            Ret
    after Timeout ->
            {error, timeout}
    end.

maybe_start_java_node() ->
    {Host, Port} = application:get_env(ekaf, ekaf_bootstrap_broker, {"localhost", "9092"}),
    %erlang:monitor_node
    ok.

get_java_node() ->
    list_to_atom("java" ++ "@" ++ inet_db:gethostname()).

is_node_alive() ->
    lists:member(get_java_node(), nodes(hidden)).

build_cmd() ->
    "java -jar " ++ jnode_jar().

jnode_jar() ->
    filename:join([code:lib_dir(ekaf),
                   "java_src","target", "JNode-1.0-jar-with-dependencies.jar"]).
