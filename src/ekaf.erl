-module(ekaf).
-export([prepare/1, produce_sync/2]).

prepare(_Topic) ->
    ok.

produce_sync(Topic, Data) ->
    ekaf_server:send(Topic,Data).
