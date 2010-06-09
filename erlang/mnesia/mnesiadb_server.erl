-module(mnesiadb_server).
-export([db/0, loop/0]).

-record(usertable, {key,
                    field0, field1, field2, field3, field4,
                    field5, field6, field7, field8, field9}).

db() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(usertable,
                        [{disc_copies, [node()]},
                         {attributes, record_info(fields, usertable)}]).

loop() ->
    register(mnesiadb_server, self()),
    loop_aux().

loop_aux() ->
    receive
        {From, init} ->
            init(From);
        {From, read, Table, Key} ->
            read(From, Table, Key);
        {From, scan, Table, Key, RecCount} ->
            scan(From, Table, Key, RecCount);
        {From, update, Table, Key, Fields} ->
            update(From, Table, Key, Fields);
        {From, insert, Table, Key, Fields} ->
            insert(From, Table, Key, Fields);
        {From, delete, Table, Key} ->
            delete(From, Table, Key)
    end,
    loop_aux().

init(From) ->
    From ! mnesia:start(),
    io:format("INIT\n", []).

read(From, Table, Key) ->
    Msg =
        case mnesia:dirty_read(Table, Key) of
            [] -> none;
            [Record] -> Record
        end,
    From ! Msg,
    io:format("READ\n", []).

scan(From, Table, Key, RecCount) ->
    Msg = scan_aux(Table, Key, RecCount),
    From ! Msg,
    io:format("SCAN\n", []).

scan_aux(_Table, _Key, 0) ->
    [];
scan_aux(Table, Key, RecCount) ->
    Msg =
        case mnesia:dirty_read(Table, Key) of
            [] -> none;
            [Record] -> Record
        end,
    NextKey = mnesia:dirty_next(Table, Key),
    [Msg|scan_aux(Table, NextKey, RecCount - 1)].

update(From, Table, Key, Fields) ->
    NewFields =
        case mnesia:dirty_read(Table, Key) of
            [] -> Fields;
            [R] ->
                ReadFields = get_read_fields(tuple_to_list(R)),
                get_new_fields(ReadFields, Fields)
        end,
    Record = erlang:make_tuple(12, undefined, [{1, Table}, {2, Key}] ++ NewFields),
    From ! mnesia:dirty_write(Record),
    io:format("UPDATE\n", []).

get_read_fields([_Table|[_Key|Fields]]) ->
    get_read_fields(Fields, 3).

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
    Record = erlang:make_tuple(12, undefined, [{1, Table}, {2, Key}] ++ Fields),
    From ! mnesia:dirty_write(Record),
    io:format("INSERT\n", []).

delete(From, Table, Key) ->
    From ! mnesia:dirty_delete(Table, Key),
    io:format("DELETE\n", []).
