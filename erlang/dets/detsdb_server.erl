
-module(detsdb_server).
-export([db/0, loop/0]).

db() ->
    dets:open_file(usertable, []),
    %% XXX: cleanup
    ok = dets:delete_all_objects(usertable).

loop() ->
    register(detsdb_server, self()),
    loop_aux().

loop_aux() ->
    receive
        {From, init} ->
            init(From);
        {From, cleanup} ->
            cleanup(From);
        {From, read, Table, Key} ->
            read(From, Table, Key);
        {From, scan, Table, Key, TupleCount} ->
            scan(From, Table, Key, TupleCount);
        {From, update, Table, Key, Fields} ->
            update(From, Table, Key, Fields);
        {From, insert, Table, Key, Fields} ->
            insert(From, Table, Key, Fields);
        {From, delete, Table, Key} ->
            delete(From, Table, Key)
    end,
    loop_aux().

init(From) ->
    From ! dets:open_file(usertable, []),
    io:format("INIT\n", []).

cleanup(From) ->
    From ! dets:close(usertable),
    io:format("CLEANUP\n", []).

read(From, Table, Key) ->
    Msg =
        case dets:lookup(Table, Key) of
            [] -> none;
            [Tuple] -> Tuple
        end,
    From ! Msg,
    io:format("READ\n", []).

scan(From, Table, Key, TupleCount) ->
    Msg = scan_aux(Table, Key, TupleCount),
    From ! Msg,
    io:format("SCAN\n", []).

scan_aux(_Table, _Key, 0) ->
    [];
scan_aux(Table, Key, TupleCount) ->
    Msg =
        case dets:lookup(Table, Key) of
            [] -> none;
            [Tuple] -> Tuple
        end,
    NextKey = dets:next(Table, Key),
    [Msg|scan_aux(Table, NextKey, TupleCount - 1)].

update(From, Table, Key, Fields) ->
    NewFields =
        case dets:lookup(Table, Key) of
            [] -> Fields;
            [T] ->
                ReadFields = get_read_fields(tuple_to_list(T)),
                get_new_fields(ReadFields, Fields)
        end,
    Tuple = erlang:make_tuple(11, undefined, [{1, Key}] ++ NewFields),
    From ! dets:insert(Table, [Tuple]),
    io:format("UPDATE\n", []).

get_read_fields([_Table|[_Key|Fields]]) ->
    get_read_fields(Fields, 2).

get_read_fields([], _Pos) ->
    [];
get_read_fields([F|Fields], Pos) ->
    FPos =
        case F =:= undefined of
            true -> [];
            false -> [{Pos, F}]
        end,
    FPos ++ get_read_fields(Fields, Pos + 1).

get_new_fields([], Fields) ->
    Fields;
get_new_fields([{Key, _Value} = F|ReadFields], Fields) ->
    NewF =
        case lists:keyfind(Key, 1, Fields) of
            false -> [F];
            _Tuple -> []
        end,
    NewF ++ get_new_fields(ReadFields, Fields).

insert(From, Table, Key, Fields) ->
    Tuple = erlang:make_tuple(11, undefined, [{1, Key}] ++ Fields),
    From ! dets:insert(Table, [Tuple]),
    io:format("INSERT\n", []).

delete(From, Table, Key) ->
    From ! dets:delete(Table, Key),
    io:format("DELETE\n", []).
