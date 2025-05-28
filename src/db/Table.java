package db;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * A {@code Table} is a collection of {@code Record}s that share the same {@code TableSchema}.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Table {

	/**
	 * The {@code TableSchema} of this {@code Table}.
	 */
	TableSchema schema;

	/**
	 * The {@code Record}s in this {@code Table}.
	 */
	Map<List<Object>, Record> records = new TreeMap<List<Object>, Record>(comparator);

	/**
	 * A {@code Comparator} for comparing {@code List<Object>}s.
	 */
	public static Comparator<List<Object>> comparator = new Comparator<List<Object>>() {

		@SuppressWarnings("unchecked")
		@Override
		public int compare(List<Object> k1, List<Object> k2) {
			for (int i = 0; i < k1.size(); i++) {
				Object o1 = k1.get(i);
				Object o2 = k2.get(i);
				int c = ((Comparable<Object>) o1).compareTo(o2);
				if (c != 0)
					return c;
			}
			return 0;
		}

	};

	/**
	 * A {@code DuplicateKeyException} is thrown if there is an attempt to insert multiple {@code Record}s with the same
	 * key into a {@code Table}.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class DuplicateKeyException extends RuntimeException {

		/**
		 * An automatically generated serial version UID.
		 */
		private static final long serialVersionUID = 8979694097821536398L;

		/**
		 * Constructs a {@code DuplicateKeyException}.
		 * 
		 * @param key
		 *            the key that caused the {@code DuplicateKeyException}
		 */
		public DuplicateKeyException(String key) {
			super(key);
		}

	}

	/**
	 * Constructs a {@code Table}.
	 * 
	 * @param schema
	 *            the {@code TableSchema} of the {@code Table}
	 */
	public Table(TableSchema schema) {
		this.schema = schema;
	}

	/**
	 * Returns a string representation of this {@code Table}.
	 * 
	 * @return a string representation of this {@code Table}
	 */
	@Override
	public String toString() {
		return "" + schema + ":" + records.size();
	}

	/**
	 * Returns the {@code TableSchema} of this {@code Table}.
	 * 
	 * @return the {@code TableSchema} of this {@code Table}
	 */
	public TableSchema schema() {
		return schema;
	}

	/**
	 * Constructs a {@code Record} containing the specified attribute values and then adds that {@code Record} to this
	 * {@code Table}.
	 * 
	 * @param values
	 *            attribute values
	 * @return the constructed {@code Record}
	 * @throws DuplicateKeyException
	 *             if this {@code Table} already contains a {@code Record} with the key of the new {@code Record} to
	 *             create
	 */
	public Record insertRecord(Object... values) {
	    Record record = new Record(schema, values);// construct values
	    List<Object> key = key(record); // Retrieve key for rec
	    if (records.containsKey(key)) {// check dup keys
	        throw new DuplicateKeyException(key.toString()); // Throw an exception if the key already exists
	    }
	    records.put(key, record);    // inset rec in map
	    return record; // ret new created record
	}

	/**
	 * Finds the specified {@code Record} from this {@code Table}.
	 * 
	 * @param key
	 *            the search key
	 * @return the {@code Record} found
	 */
	public Record find(Object... key) {
		return records.get(List.of(key));
	}

	/**
	 * Finds the {@code Record}s in this {@code Table} that match the specified {@code Record}.
	 * 
	 * @param r
	 *            a {@code Record}
	 * @param commonAttributes
	 *            the names of the common attributes of the {@code Record}s
	 * @return the {@code Record}s in this {@code Table} that match the specified {@code Record}
	 */
	public Collection<Record> matchingRecords(Record r, Set<String> commonAttributes) {
		if (schema.key != null && commonAttributes.containsAll(schema.key)) {
			var k = key(r);
			var record = records.get(k);
			if (record != null && matching(r, record, commonAttributes))
				return List.of(record);
			return List.of();
		}
		var l = new LinkedList<Record>();
		for (var record : records.values())
			if (matching(r, record, commonAttributes))
				l.add(record);
		return l;
	}

	/**
	 * Finds, from the specified {@code Record}, the values of the attributes that correspond to the primary key.
	 * 
	 * @param r
	 *            a {@code Record}
	 * @return the values of the attributes that correspond to the primary key
	 */
	private List<Object> key(Record r) {
		return r.values(schema.key.toArray(new String[0]));
	}

	/**
	 * Determines whether or not the two specified {@code Record}s match each other (i.e., have the same value for each
	 * common attribute).
	 * 
	 * @param r1
	 *            a {@code Record}
	 * @param r2
	 *            a {@code Record}
	 * @param commonAttributes
	 *            the common attributes of these {@code Record}s
	 * @return {@code true} if the two specified {@code Record}s match each other (i.e., have the same value for each
	 *         common attribute); {@code false} otherwise
	 */
	private boolean matching(Record r1, Record r2, Set<String> commonAttributes) {
		for (var a : commonAttributes)
			if (!r1.value(a).equals(r2.value(a)))
				return false;
		return true;
	}

	public Collection<Record> getRecords() {
	    return records.values();
	}

}
