-module(ejkaf).
-export([prepare/1, produce_sync/2,
         become_consumer/1,
         become_consumer/2,
         recv/0,
         recv/1
        ]).

prepare(_Topic) ->
    ejkaf_server:prepare().


produce_sync(Topic, Data) ->
    ejkaf_server:send(Topic,Data).

recv() ->
    ejkaf_server:recv().
recv(T) ->
    ejkaf_server:recv(T).
become_consumer(Topic) ->
    ejkaf_server:become_consumer(Topic).
become_consumer(Topic,T) ->
    ejkaf_server:become_consumer(Topic,T).
