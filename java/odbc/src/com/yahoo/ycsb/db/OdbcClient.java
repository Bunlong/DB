/**
 * ODBC client for YCSB framework.
 *
 * Created by Maria Christakis on 10/6/2010.
 */

package com.yahoo.ycsb.db;

/**
 * YCSB related imports
 */
import com.yahoo.ycsb.*;
import java.util.Set;
import java.util.HashMap;
import java.util.Vector;

/**
 * Erlang related imports
 */
import com.ericsson.otp.erlang.*;

/**
 * Java/Erlang interface related imports
 */
import java.util.Iterator;

public class OdbcClient extends DB {

    private OtpNode node = null;
    private OtpMbox mBox = null;
    private OtpErlangPid conn = null;
    private String user = "maria@artemis";
    private String server = "odbcdb_server";

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException {
        try {
            node = new OtpNode("node");
            mBox = node.createMbox();
            OtpErlangTuple msg =
                new OtpErlangTuple(
                                   new OtpErlangObject[] {
                                       mBox.self(),
                                       new OtpErlangAtom("init")
                                   });
            mBox.send(server, user, msg);
            conn = (OtpErlangPid) mBox.receive();
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException {
        try {
            OtpErlangTuple msg =
                new OtpErlangTuple(
                                   new OtpErlangObject[] {
                                       mBox.self(),
                                       new OtpErlangAtom("cleanup"),
                                       conn
                                   });
            mBox.send(server, user, msg);
            OtpErlangAtom recAtom = (OtpErlangAtom) mBox.receive();
            OtpErlangAtom okAtom = new OtpErlangAtom("ok");
            assert recAtom.equals(okAtom);
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields,
                    HashMap<String,String> result) {

        boolean returnAllFields = fields == null;

        try {
            OtpErlangString tableStr = new OtpErlangString(table);
            OtpErlangString keyStr = new OtpErlangString(key);
            OtpErlangTuple msg =
                new OtpErlangTuple(
                                   new OtpErlangObject[] {
                                       mBox.self(),
                                       new OtpErlangAtom("read"),
                                       conn,
                                       tableStr,
                                       keyStr
                                   });
            mBox.send(server, user, msg);
            OtpErlangObject received = mBox.receive();
            OtpErlangAtom noneAtom = new OtpErlangAtom("none");
            if (received.equals(noneAtom))
                return 1;
            else {
                if (!returnAllFields) {
                    Iterator<String> itr = fields.iterator();
                    while (itr.hasNext()) {
                        int i = Integer.parseInt(itr.next().substring(5));
                        String value =
                            ((OtpErlangTuple) received).elementAt(i + 1).toString();
                        result.put(new String("field" + i), new String(value));
                    }
                }
                else {
                    int recArity = ((OtpErlangTuple) received).arity();
                    for (int i = 0; i < recArity - 1; i++) {
                        String value =
                            ((OtpErlangTuple) received).elementAt(i + 1).toString();
                        result.put(new String("field" + i), new String(value));
                    }
                }
            }
            return 0;
        }
        catch (Exception e) {
            System.out.println(e);
            return 1;
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String,String>> result) {

        boolean returnAllFields = fields == null;

        try {
            OtpErlangString tableStr = new OtpErlangString(table);
            OtpErlangString keyStr = new OtpErlangString(startkey);
            OtpErlangString recCountStr =
                new OtpErlangString(Integer.toString(recordcount));
            OtpErlangTuple msg =
                new OtpErlangTuple(
                                   new OtpErlangObject[] {
                                       mBox.self(),
                                       new OtpErlangAtom("scan"),
                                       conn,
                                       tableStr,
                                       keyStr,
                                       recCountStr
                                   });
            mBox.send(server, user, msg);
            OtpErlangList received = (OtpErlangList) mBox.receive();
            OtpErlangAtom noneAtom = new OtpErlangAtom("none");
            Iterator<OtpErlangObject> itr1 = received.iterator();
            while (itr1.hasNext()) {
                OtpErlangObject recElem = itr1.next();
                if (recElem.equals(noneAtom))
                    return 1;
                else {
                    HashMap<String,String> tuple = new HashMap<String, String>();
                    if (!returnAllFields) {
                        Iterator<String> itr2 = fields.iterator();
                        while (itr2.hasNext()) {
                            int i = Integer.parseInt(itr2.next().substring(5));
                            String value =
                                ((OtpErlangTuple) recElem).elementAt(i + 1).toString();
                            tuple.put(new String("field" + i), new String(value));
                        }
                    }
                    else {
                        int recArity = ((OtpErlangTuple) recElem).arity();
                        for (int i = 0; i < recArity - 1; i++) {
                            String value =
                                ((OtpErlangTuple) recElem).elementAt(i + 1).toString();
                            tuple.put(new String("field" + i), new String(value));
                        }
                    }
                    result.add(tuple);
                }
            }
            return 0;
        }
        catch (Exception e) {
            System.out.println(e);
            return 1;
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String,String> values) {
        try {
            OtpErlangString tableStr = new OtpErlangString(table);
            OtpErlangString keyStr = new OtpErlangString(key);
            OtpErlangObject fieldsObj[] = new OtpErlangObject[values.size()];
            OtpErlangObject valuesObj[] = new OtpErlangObject[values.size()];
            int i = 0;
            for (String field : values.keySet()) {
                fieldsObj[i] = new OtpErlangString(field);
                valuesObj[i] = new OtpErlangString(values.get(field));
                i++;
            }
            OtpErlangTuple msg =
                new OtpErlangTuple(
                                   new OtpErlangObject[] {
                                       mBox.self(),
                                       new OtpErlangAtom("update"),
                                       conn,
                                       tableStr,
                                       keyStr,
                                       new OtpErlangList(fieldsObj),
                                       new OtpErlangList(valuesObj)
                                   });
            mBox.send(server, user, msg);
            OtpErlangTuple recTuple = (OtpErlangTuple) mBox.receive();
            OtpErlangTuple upTuple =
                new OtpErlangTuple(
                                   new OtpErlangObject[] {
                                       new OtpErlangAtom("updated"),
                                       new OtpErlangInt(1)
                                   });
            assert recTuple.equals(upTuple);
            return 0;
        }
        catch (Exception e) {
            System.out.println(e);
            return 1;
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the
     * specified values HashMap will be written into the record with the
     * specified record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String,String> values) {
        try {
            OtpErlangString tableStr = new OtpErlangString(table);
            OtpErlangString keyStr = new OtpErlangString(key);
            OtpErlangObject fieldsObj[] = new OtpErlangObject[values.size()];
            OtpErlangObject valuesObj[] = new OtpErlangObject[values.size()];
            int i = 0;
            for (String field : values.keySet()) {
                fieldsObj[i] = new OtpErlangString(field);
                valuesObj[i] = new OtpErlangString(values.get(field));
                i++;
            }
            OtpErlangTuple msg =
                new OtpErlangTuple(
                                   new OtpErlangObject[] {
                                       mBox.self(),
                                       new OtpErlangAtom("insert"),
                                       conn,
                                       tableStr,
                                       keyStr,
                                       new OtpErlangList(fieldsObj),
                                       new OtpErlangList(valuesObj)
                                   });
            mBox.send(server, user, msg);
            OtpErlangTuple recTuple = (OtpErlangTuple) mBox.receive();
            OtpErlangTuple upTuple =
                new OtpErlangTuple(
                                   new OtpErlangObject[] {
                                       new OtpErlangAtom("updated"),
                                       new OtpErlangInt(1)
                                   });
            assert recTuple.equals(upTuple);
            return 0;
        }
        catch (Exception e) {
            System.out.println(e);
            return 1;
        }
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key) {
        try {
            OtpErlangString tableStr = new OtpErlangString(table);
            OtpErlangString keyStr = new OtpErlangString(key);
            OtpErlangTuple msg =
                new OtpErlangTuple(
                                   new OtpErlangObject[] {
                                       mBox.self(),
                                       new OtpErlangAtom("delete"),
                                       conn,
                                       tableStr,
                                       keyStr
                                   });
            mBox.send(server, user, msg);
            OtpErlangTuple recTuple = (OtpErlangTuple) mBox.receive();
            OtpErlangTuple upTuple =
                new OtpErlangTuple(
                                   new OtpErlangObject[] {
                                       new OtpErlangAtom("updated"),
                                       new OtpErlangInt(1)
                                   });
            assert recTuple.equals(upTuple);
            return 0;
        }
        catch (Exception e) {
            System.out.println(e);
            return 1;
        }
    }
}
