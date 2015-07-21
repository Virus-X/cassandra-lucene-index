package com.stratio.cassandra.lucene.schema.mapping;

import com.stratio.cassandra.lucene.schema.column.Column;
import com.stratio.cassandra.lucene.schema.column.Columns;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import com.stratio.cassandra.lucene.util.Log;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.Hex;
import org.apache.lucene.document.*;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by vgoncharenko on 20.07.2015.
 */
public class MsgPackMapper extends Mapper {
    /** The default limit for depth of message pack fields. */
    private static final int DEFAULT_DEPTH_LIMIT = 2;

    /** The default case sensitive option. */
    public static final boolean DEFAULT_CASE_SENSITIVE = true;

    /** If it must be case sensitive. */
    private final boolean caseSensitive;

    public MsgPackMapper(String name, Boolean indexed, Boolean sorted, Boolean caseSensitive){
        super(name,
              indexed,
              sorted,
              AsciiType.instance,
              UTF8Type.instance,
              BytesType.instance);

        this.caseSensitive = caseSensitive == null ? DEFAULT_CASE_SENSITIVE : caseSensitive;
    }

    @Override
    public void addFields(Document document, Columns columns) {
        for (Column column : columns.getColumnsByName(name)) {
            String name = column.getFullName();
            Log.info("n:" + name);
            Log.info("column: " + column);

            for(Field f : extractFields(name, Unpack(column.getComposedValue()), 0)){
                document.add(f);
            }
        }
    }

    @Override
    public SortField sortField(boolean reverse) {
        return new SortField(name, SortField.Type.STRING, reverse);
    }

    @Override
    public void validate(CFMetaData metaData) {

    }

    public int compare(Column col1, Column col2){
        if (col1 == null)
        {
            return col2 == null ? 0 : 1;
        }
        if (col2 == null)
        {
            return -1;
        }

        return CompareValues(
                Unpack(col1.getComposedValue()),
                Unpack(col2.getComposedValue()));
    }

    private List<Field> extractFields(String name, Value v, int depth){
        ArrayList<Field> fields = new ArrayList<Field>();
        switch (v.getValueType()) {
            case NIL:
            case EXTENSION:
                // NOT Supported
                break;
            case BOOLEAN:
                fields.add(new StringField(name, v.asBooleanValue().getBoolean() ? "true" : "false", STORE));
                break;
            case INTEGER:
            case FLOAT:
                fields.add(new DoubleField(name, v.asNumberValue().toDouble(), STORE));
                break;
            case STRING:
                String data = caseSensitive
                        ? v.asStringValue().toString()
                        : v.asStringValue().toString().toLowerCase();

                fields.add(new StringField(name, data, STORE));
                break;
            case BINARY:
                fields.add(new StringField(name, Hex.bytesToHex(v.asBinaryValue().asByteArray()), STORE));
                break;
            case ARRAY:
                for (Value val : v.asArrayValue())                {
                    fields.addAll(extractFields(name, val, depth));
                }
                break;
            case MAP:
                if (depth >= DEFAULT_DEPTH_LIMIT){
                    // Depth limit reached
                    break;
                }

                for (Map.Entry<Value,Value> kvp : v.asMapValue().entrySet()){
                    Value key = kvp.getKey();
                    if (key.isNumberValue() || key.isBooleanValue() || key.isStringValue()){
                        String keyName = name + "." + key.toString();
                        fields.addAll(extractFields(keyName, kvp.getValue(), depth + 1));
                    }else{
                        // Key cannot have type map or array
                        continue;
                    }
                }
                break;
        }

        return fields;
    }

    private int CompareValues(Value a, Value b){
        if (a == null || a.isNilValue())
        {
            return b == null || b.isNilValue() ? 0 : -1;
        }
        if (b == null || b.isNilValue())
        {
            return 1;
        }

        Log.info("comparing " + a + " & " + b);

        switch (a.getValueType())
        {
            case INTEGER:
            case FLOAT:
                if (b.getValueType() == ValueType.INTEGER || b.getValueType() == ValueType.FLOAT){
                    return Double.compare(a.asNumberValue().toDouble(), b.asNumberValue().toDouble());
                }else {
                    // Assume numbers are always < than other types
                    return -1;
                }
            case BOOLEAN:
            case STRING:
                if (b.getValueType() == ValueType.INTEGER || b.getValueType() == ValueType.FLOAT){
                    return 1;
                }else if (b.getValueType() == ValueType.STRING || b.getValueType() == ValueType.BOOLEAN){
                    String stringA = a.getValueType() == ValueType.STRING ? a.asStringValue().toString() : a.asBooleanValue().toString();
                    String stringB = b.getValueType() == ValueType.STRING ? b.asStringValue().toString() : b.asBooleanValue().toString();

                    return this.caseSensitive
                            ? stringA.compareTo(stringB)
                            : stringA.compareToIgnoreCase(stringB);

                }else{
                    // Assume strings are always < arrays, maps, and complex objects.
                    return -1;
                }
            default:
                if (b.getValueType() == ValueType.INTEGER ||
                        b.getValueType() == ValueType.FLOAT ||
                        b.getValueType() == ValueType.STRING ||
                        b.getValueType() == ValueType.BOOLEAN)
                {
                    return 1;
                }else{
                    return 0;
                }
        }
    }

    private ImmutableValue Unpack(Object value){
        Log.info("value: " + value);
        byte [] data = asBytes(value);
        try {
            return org.msgpack.core.MessagePack.newDefaultUnpacker(data).unpackValue();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private byte[] asBytes(Object value) {
        Log.info("Value type: " + value.getClass().getName());

        if (value == null) {
            Log.info("Value null");
            return null;
        }
        else if (value instanceof ByteBuffer) {
            Log.info("Byte buffer!!");
            ByteBuffer bb = (ByteBuffer) value;
            return ByteBufferUtils.asArray(bb);
        } else if (value instanceof byte[]) {
            Log.info("Byte array!!");
            return (byte[]) value;
        } else if (value instanceof String) {
            String string = (String) value;

            Log.info("String: " + string);
            string = string.replaceFirst("0x", "");
            return Hex.hexToBytes(string);
        } else {
            throw new IllegalArgumentException(String.format("Value '%s' cannot be cast to byte array", value));
        }
    }
}
