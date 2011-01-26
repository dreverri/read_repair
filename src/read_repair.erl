-module(read_repair).

-export([main/1]).

-ifdef(TEST).
-compile(export_all).
-endif.

main([Ip, Port|Buckets]) ->
    PortInt = list_to_integer(Port),
    {ok, Pid} = riakc_pb_socket:start_link(Ip, PortInt),
    run(Pid, Buckets);

main(_) ->
    io:format("Usage: read_repair pb_ip port [<bucket>]~n", []).

run(Pid, Buckets) ->
    [read_repair_bucket(Pid, list_to_binary(Bucket)) || Bucket <- Buckets].

read_repair_bucket(Pid, Bucket) ->
    error_logger:info_msg("starting read repair on ~s~n", [Bucket]),
    riakc_pb_socket:stream_list_keys(Pid, Bucket),
    wait_for_keys(Pid, Bucket).

wait_for_keys(Pid, Bucket) ->
    receive
        {_, {keys, Keys}} ->
            error_logger:info_msg("reading ~p keys~n", [length(Keys)]),
            read(Pid, Bucket, Keys),
            wait_for_keys(Pid, Bucket);
        {_, done} ->
            error_logger:info_msg("~s done~n", [Bucket]);
        Unknown ->
            error_logger:error_msg("Stopping; unknown message received: ~p~n", [Unknown]),
            halt(1)
    end.

read(_, _, []) ->
    ok;

read(Pid, Bucket, [Key|Keys]) ->
    riakc_pb_socket:get(Pid, Bucket, Key, [{r,1}]),
    read(Pid, Bucket, Keys).
