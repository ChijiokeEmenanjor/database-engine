package db;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * A {@code TableSchema} represents the schema of a table.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class TableSchema {

	/**
	 * The mapping from each attribute name to attribute index.
	 */
	Map<String, Integer> attributeIndices = new LinkedHashMap<String, Integer>();

	/**
	 * The names of the attributes that constitute the primary key of this {@code TableSchema}.
	 */
	List<String> key = List.of();

	/**
	 * A {@code DuplicateAttributeNameException} is thrown if there is an attempt to include multiple attributes with
	 * the same name to a {@code TableSchema}.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class DuplicateAttributeNameException extends RuntimeException {

		/**
		 * An automatically generated serial version UID.
		 */
		private static final long serialVersionUID = -6346797088300026496L;

		/**
		 * Constructs a {@code DuplicateAttributeNameException}.
		 * 
		 * @param attributeName
		 *            the name of the attribute that caused the {@code DuplicateAttributeNameException}
		 */
		public DuplicateAttributeNameException(String attributeName) {
			super(attributeName);
		}

	}

	/**
	 * Constructs an empty {@code TableSchema}.
	 */
	public TableSchema() {
	}

	/**
	 * Constructs a {@code TableSchema} by combining the two specified {@code TableSchema}s.
	 * 
	 * @param schema1
	 *            a {@code TableSchema}
	 * @param schema2
	 *            a {@code TableSchema}
	 */
	public TableSchema(TableSchema schema1, TableSchema schema2) {
		for (var attributeName : schema1.attributeIndices.keySet())
			this.attribute(attributeName);
		for (var attributeName : schema2.attributeIndices.keySet())
			if (!this.attributeIndices.containsKey(attributeName))
				this.attribute(attributeName);
	}

	/**
	 * Adds an attribute to this {@code TableSchema}.
	 * 
	 * @param attributeName
	 *            the name of the attribute
	 * @return this {@code TableSchema}
	 * @throws DuplicateAttributeNameException
	 *             if the specified attribute name is already registered in this {@code TableSchema}
	 */
	public TableSchema attribute(String attributeName) {
	    if (attributeIndices.containsKey(attributeName)) {
	        throw new DuplicateAttributeNameException(attributeName);
	    }
	    attributeIndices.put(attributeName, attributeIndices.size());
	    return this;
	}
	/**
	 * Sets the primary key of this {@code TableSchema}.
	 * 
	 * @param key
	 *            the primary key of this {@code TableSchema}
	 * @return this {@code TableSchema}
	 */
	public TableSchema key(String... key) {
		this.key = List.of(key);
		return this;
	}

	/**
	 * Returns a string representation of this {@code TableSchema}.
	 * 
	 * @return a string representation of this {@code TableSchema}
	 */
	@Override
	public String toString() {
		var m = new TreeMap<Integer, String>();
		attributeIndices.entrySet().stream()
				.forEach(e -> m.put(e.getValue(), e.getKey()));
		return "{attributes=" + attributeIndices + ", key=" + key + "}";
	}

	/**
	 * Returns the number of attributes in this {@code TableSchema}.
	 * 
	 * @return the number of attributes in this {@code TableSchema}
	 */
	public int size() {
		return attributeIndices.size();
	}

	/**
	 * Returns the index of the specified attribute in this {@code TableSchema} ({@code null} if there is no such
	 * attribute).
	 * 
	 * @param attributeName
	 *            the name of the attribute
	 * @return the index of the specified attribute in this {@code TableSchema}; {@code null} if there is no such
	 *         attribute
	 */
	public Integer attributeIndex(String attributeName) {
		return attributeIndices.get(attributeName);
	}

	/**
	 * Returns the names of the common attributes between this {@code TableSchema} and the specified
	 * {@code TableSchema}.
	 * 
	 * @param schema
	 *            a {@code TableSchema}
	 * @return the names of the common attributes between this {@code TableSchema} and the specified {@code TableSchema}
	 */
	public Set<String> commonAttributeNames(TableSchema schema) {
		var commonAttributes = new LinkedHashSet<String>(attributeIndices.keySet());
		commonAttributes.retainAll(schema.attributeIndices.keySet());
		return commonAttributes;
	}

}
