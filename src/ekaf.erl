-module(ekaf).
-export([prepare/1, produce_sync/2, recv/1]).

prepare(_Topic) ->
    ejkaf_server:prepare().


produce_sync(Topic, Data) ->
    ejkaf_server:send(Topic,Data).

recv(Topic) ->
    ejkaf_server:recv(Topic).
