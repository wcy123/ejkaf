-module(ekaf).
-export([prepare/1, produce_sync/2]).

prepare(_Topic) ->
    ekaf_server:prepare().


produce_sync(Topic, Data) ->
    ekaf_server:send(Topic,Data).
