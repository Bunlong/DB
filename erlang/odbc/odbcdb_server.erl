-module(odbcdb_server).
-export([db/0, loop/0]).

-define(ConnString, "DSN=mysqldb").
-define (Columns, ["key_id",
                   "field0", "field1", "field2", "field3", "field4",
                   "field5","field6","field7","field8", "field9"]).

db() ->
    application:start(odbc),
    {ok, Conn} = odbc:connect(?ConnString, []),
    %% XXX: cleanup
    odbc:sql_query(Conn, "DROP TABLE usertable"),
    odbc:sql_query(Conn,
                   "CREATE TABLE usertable ("
                   "key_id VARCHAR(100),"
                   "field0 VARCHAR(100),"
                   "field1 VARCHAR(100),"
                   "field2 VARCHAR(100),"
                   "field3 VARCHAR(100),"
                   "field4 VARCHAR(100),"
                   "field5 VARCHAR(100),"
                   "field6 VARCHAR(100),"
                   "field7 VARCHAR(100),"
                   "field8 VARCHAR(100),"
                   "field9 VARCHAR(100),"
                   "PRIMARY KEY (key_id)"
                   ")"),
    odbc:disconnect(Conn).

loop() ->
    register(odbcdb_server, self()),
    loop_aux().

loop_aux() ->
    receive
        {From, init} ->
            init(From);
        {From, cleanup, Conn} ->
            cleanup(From, Conn);
        {From, read, Conn, Table, Key} ->
            read(From, Conn, Table, Key);
        {From, scan, Conn, Table, Key, RecCount} ->
            scan(From, Conn, Table, Key, RecCount);
        {From, update, Conn, Table, Key, Fields, Values} ->
            update(From, Conn, Table, Key, Fields, Values);
        {From, insert, Conn, Table, Key, Fields, Values} ->
            insert(From, Conn, Table, Key, Fields, Values);
        {From, delete, Conn, Table, Key} ->
            delete(From, Conn, Table, Key)
    end,
    loop_aux().

init(From) ->
    {ok, Conn} = odbc:connect(?ConnString, []),
    From ! Conn,
    io:format("INIT\n", []).

cleanup(From, Conn) ->
    From ! odbc:disconnect(Conn),
    io:format("CLEANUP\n", []).

read(From, Conn, Table, Key) ->
    Query = io_lib:format("SELECT * FROM ~s WHERE key_id=\"~s\"",
                          [Table, Key]),
    {selected, ?Columns, Selection} = odbc:sql_query(Conn, Query),
    Msg =
        case Selection of
            [] -> none;
            [Tuple] -> Tuple
        end,
    From ! Msg,
    io:format("READ\n", []).

scan(From, Conn, Table, Key, RecCount) ->
    Query = io_lib:format("SELECT * FROM ~s WHERE key_id>=\"~s\" LIMIT ~s",
                          [Table, Key, RecCount]),
    {selected, ?Columns, Selection} = odbc:sql_query(Conn, Query),
    Msg =
        case Selection of
            [] -> none;
            Tuples -> Tuples
        end,
    From ! Msg,
    io:format("SCAN\n", []).

update(From, Conn, Table, Key, Fields, Values) ->
    FieldValueStr = get_field_eq_value_str(Fields, Values),
    Query = io_lib:format("UPDATE ~s SET ~s WHERE key_id=\"~s\"",
                          [Table, FieldValueStr, Key]),
    From ! odbc:sql_query(Conn, Query),
    io:format("INSERT\n", []).

get_field_eq_value_str([], []) ->
    "";
get_field_eq_value_str([F], [V]) ->
    io_lib:format("~s = \"~s\"", [F, V]);
get_field_eq_value_str([F|Fields], [V|Values]) ->
    [io_lib:format("~s = \"~s\", ", [F, V])|
     get_field_eq_value_str(Fields, Values)].

insert(From, Conn, Table, Key, Fields, Values) ->
    {FieldStr, ValueStr} = get_fields_values_str(["key_id"|Fields],
                                                 [Key|Values]),
    Query = io_lib:format("INSERT INTO ~s (~s) VALUES(~s)",
                          [Table, FieldStr, ValueStr]),
    From ! odbc:sql_query(Conn, Query),
    io:format("INSERT\n", []).

get_fields_values_str([], []) ->
    {"", ""};
get_fields_values_str([F], [V]) ->
    V1 = io_lib:format("\"~s\"", [V]),
    {F, V1};
get_fields_values_str([F|Fields], [V|Values]) ->
    F1 = io_lib:format("~s, ", [F]),
    V1 = io_lib:format("\"~s\", ", [V]),
    {FStr, VStr} = get_fields_values_str(Fields, Values),
    {[F1|FStr], [V1|VStr]}.

delete(From, Conn, Table, Key) ->
    Query = io_lib:format("DELETE FROM ~s WHERE key_id=\"~s\"",
                          [Table, Key]),
    From ! odbc:sql_query(Conn, Query),
    io:format("DELETE\n", []).
