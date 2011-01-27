-module(read_repair).

-export([main/1]).

-ifdef(TEST).
-compile(export_all).
-endif.

-define(FMT(Str, Args), lists:flatten(io_lib:format(Str, Args))).

main([Ip, Port|Buckets]) ->
    {ok, Cwd} = file:get_cwd(),
    TestId = id(),
    LogFile = filename:join([Cwd, "repairs", TestId, "log.txt"]),
    ok = filelib:ensure_dir(LogFile),
    {ok, Io} = file:open(LogFile, [write]),
    io:format(Io, "bucket, key, error~n", []),

    PortInt = list_to_integer(Port),
    {ok, Pid} = riakc_pb_socket:start_link(Ip, PortInt),
    run(Pid, Buckets, Io);

main(_) ->
    io:format("Usage: read_repair pb_ip port [<bucket>]~n", []).

run(Pid, Buckets, Io) ->
    [read_repair_bucket(Pid, list_to_binary(Bucket), Io) || Bucket <- Buckets].

read_repair_bucket(Pid, Bucket, Io) ->
    error_logger:info_msg("starting read repair on ~s~n", [Bucket]),
    riakc_pb_socket:stream_list_keys(Pid, Bucket, infinity, infinity),
    wait_for_keys(Pid, Bucket, Io).

wait_for_keys(Pid, Bucket, Io) ->
    receive
        {_, {keys, Keys}} ->
            error_logger:info_msg("reading ~p keys~n", [length(Keys)]),
            read(Pid, Bucket, Keys, Io),
            wait_for_keys(Pid, Bucket, Io);
        {_, done} ->
            error_logger:info_msg("~s done~n", [Bucket]);
        Unknown ->
            erlang:exit("Stopping; unknown message received: ~p~n", [Unknown])
    end.

read(_, _, [], _) ->
    ok;

read(Pid, Bucket, [Key|Keys], Io) ->
    case riakc_pb_socket:get(Pid, Bucket, Key, [{r,1}], infinity) of
        {error, Reason} ->
            error_logger:error_report([{bucket, Bucket}, {key, Key}, {reason, Reason}]),
            ok = io:format(Io, "~p, ~p, ~p~n", [Bucket, Key, Reason]),
            file:sync(Io);
        _ ->
            ignore
    end,
    read(Pid, Bucket, Keys, Io).

id() ->
    {{Y, M, D}, {H, Min, S}} = calendar:local_time(),
    ?FMT("~w~2..0w~2..0w_~2..0w~2..0w~2..0w", [Y, M, D, H, Min, S]).
