package db;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * A {@code Record} represents a record which contains a number of attributes.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Record {

    /**
     * The {@code TableSchema} for this {@code Record}.
     */
    TableSchema schema;

    /**
     * The attribute values of this {@code Record}.
     */
    Object[] values;

    /**
     * Constructs a {@code Record}.
     * 
     * @param schema a {@code TableSchema} that the {@code Record} must be in compliance with
     * @param values the attribute values of the {@code Record}
     */
    public Record(TableSchema schema, Object... values) {
        this.schema = schema;
        if (values.length != schema.size()) {//values are checked to see if schema size is matched
            throw new IllegalArgumentException("Number of values does not match the schema size");
        }
        
        this.values = new Object[schema.size()];
        System.arraycopy(values, 0, this.values, 0, values.length); // values moved into array
    }

    /**
     * Constructs a {@code Record}.
     * 
     * @param schema a {@code TableSchema} that the {@code Record} must be in compliance with
     * @param values the attribute values of the {@code Record}
     */
    public Record(TableSchema schema, List<Object> values) {
        this(schema, values.toArray()); // constructor values is in initializeed
    }

    /**
     * Concatenate the two specified {@code Record}s.
     * 
     * @param r1 a {@code Record}
     * @param r2 a {@code Record}
     * @param schema the {@code TableSchema} to use when concatenating the two {@code Record}s
     * @return a new {@code Record} obtained by concatenating the two {@code Record}s
     */
    public static Record concatenate(Record r1, Record r2, TableSchema schema) {
        var values = new LinkedList<Object>();
        for (var attributeName : schema.attributeIndices.keySet()) {
            var value = r1.value(attributeName);
            if (value == null) {
                value = r2.value(attributeName);
            }
            values.add(value);
        }
        return new Record(schema, values.toArray());
    }

    /**
     * Returns a string representation of this {@code Record}.
     * 
     * @return a string representation of this {@code Record}
     */
    @Override
    public String toString() {
        var m = new LinkedHashMap<String, Object>();
        schema.attributeIndices.keySet().stream()
                .forEach(n -> m.put(n, values[schema.attributeIndices.get(n)]));
        return m.toString();
    }

    /**
     * Returns the value of the specified attribute by index.
     * 
     * @param attributeIndex the index of an attribute
     * @return the value of the specified attribute
     */
    public Object value(int attributeIndex) {
        if (attributeIndex < 0 || attributeIndex >= values.length) {
            throw new IndexOutOfBoundsException("Invalid attribute index: " + attributeIndex);
        }
        return values[attributeIndex];
    }

    /**
     * Returns the value of the specified attribute by name.
     * 
     * @param attributeName the name of an attribute
     * @return the value of the specified attribute
     */
    public Object value(String attributeName) {
        Integer index = schema.attributeIndex(attributeName);
        if (index == null) {
            return null;
        }
        return value(index);
    }

    /**
     * Returns the values of the specified attributes.
     * 
     * @param attributeNames the names of the attributes
     * @return the values of the specified attributes
     */
    public List<Object> values(String... attributeNames) {
        if (attributeNames == null) {
            return null;
        }
        return IntStream.range(0, attributeNames.length)
                .mapToObj(i -> value(attributeNames[i])).toList();}}

