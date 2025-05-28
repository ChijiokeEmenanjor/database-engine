package db;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import db.expression.ArithmeticExpression;
import db.expression.ParsingException;

/**
 * A {@code Database} is a collection of {@code Table}s.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Database {

	/**
	 * The name of this {@code Database}.
	 */
	String databaseName;

	/**
	 * The {@code Table}s in this {@code Database}.
	 */
	Map<String, Table> tables = new TreeMap<String, Table>();

	/**
	 * Constructs a {@code Database}.
	 * 
	 * @param databaseName
	 *            the name of the {@code Database}
	 */
	public Database(String databaseName) {
		this.databaseName = databaseName;
	}

	/**
	 * Constructs a table in this {@code Database}.
	 * 
	 * @param tableName
	 *            the name of the table
	 * @return the {@code TableSchema} of the table
	 */
	public TableSchema createTable(String tableName) {
		var schema = new TableSchema();
		tables.put(tableName, new Table(schema));
		return schema;
	}

	/**
	 * Returns the {@code Table} having the specified name.
	 * 
	 * @param name
	 *            the name of the {@code Table}
	 * @return the {@code Table} having the specified name
	 */
	public Table table(String name) {
		return tables.get(name);
	}

	/**
	 * Returns a string representation of this {@code Database}.
	 * 
	 * @return a string representation of this {@code Database}
	 */
	@Override
	public String toString() {
		return databaseName + tables;
	}

	/**
	 * Returns a {@code Stream} of {@code Record}s that contain the specified attributes and are generated from the
	 * specified tables.
	 * 
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @return a {@code Stream} of {@code Record}s that contain the specified attributes and are generated from the
	 *         specified tables
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public Stream<Record> select(String attributeDefinitions, String tableNames) throws ParsingException {
		return query(attributeDefinitions, tableNames, null, null).stream();
	}

	/**
	 * Returns a {@code Stream} of {@code Record}s that contain the specified attributes and are generated from the
	 * specified tables using the specified predicate.
	 * 
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @param predicate
	 *            a predicate
	 * @return a {@code Stream} of {@code Record}s that contain the specified attributes and are generated from the
	 *         specified tables using the specified predicate
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public Stream<Record> select(String attributeDefinitions, String tableNames, String predicate)
			throws ParsingException {
		return query(attributeDefinitions, tableNames, predicate, null).stream();
	}

	/**
	 * Returns a {@code Stream} of {@code Record}s that contain the specified attributes and are generated from the
	 * specified tables using the specified grouping attributes.
	 * 
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @param groupingAttributes
	 *            the names of the attributes used for grouping
	 * @return a {@code Stream} of {@code Record}s that contain the specified attributes and are generated from the
	 *         specified tables using the specified grouping attributes
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public Stream<Record> selectGroupBy(String attributeDefinitions, String tableNames, String groupingAttributes)
			throws ParsingException {
		return query(attributeDefinitions, tableNames, null, groupingAttributes).stream();
	}

	/**
	 * Returns a {@code Stream} of {@code Record}s that contain the specified attributes and are generated from the
	 * specified tables using the specified predicate and grouping attributes.
	 * 
	 * @param attributeDefinitions
	 *            the definitions of attributes
	 * @param tableNames
	 *            the names of the tables
	 * @param predicate
	 *            a predicate
	 * @param groupingAttributes
	 *            the names of the attributes used for grouping
	 * @return a {@code Stream} of {@code Record}s that contain the specified attributes and are generated from the
	 *         specified tables using the specified predicate and grouping attributes
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	public Stream<Record> selectGroupBy(String attributeDefinitions, String tableNames, String predicate,
			String groupingAttributes) throws ParsingException {
		return query(attributeDefinitions, tableNames, predicate, groupingAttributes).stream();
	}

	/**
	 * Returns an {@code Operator} that provides {@code Record}s containing the specified attributes and generated using
	 * the specified tables, predicate, and grouping attributes.
	 * 
	 * @param attributeDefinitions
	 *            a string representation of attribute definitions
	 * @param tableNames
	 *            a string representation of table names
	 * @param predicate
	 *            a predicate
	 * @param groupingAttributes
	 *            a string representation of the attributes used for grouping
	 * @return an {@code Operator} that provides {@code Record}s containing the specified attributes and generated using
	 *         the specified tables predicate, and grouping attributes
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	private Operator query(String attributeDefinitions, String tableNames, String predicate, String groupingAttributes)
			throws ParsingException {
		return query(Arrays.stream(attributeDefinitions.split(",")).map(s -> s.trim()).toList(),
				Arrays.stream(tableNames.split("natural join")).map(s -> s.trim()).toList(), predicate,
				groupingAttributes == null ? null
						: Arrays.stream(groupingAttributes.split(",")).map(s -> s.trim()).toList());
	}

	/**
	 * Returns an {@code Operator} that provides {@code Record}s that contain the specified attributes and that are
	 * generated using the specified tables, predicate, and grouping attributes.
	 * 
	 * @param attributeDescriptions
	 *            a {@code List} of attributes
	 * @param tableNames
	 *            a {@code List} of table names
	 * @param predicate
	 *            a predicate
	 * @param groupingAttributes
	 *            the names of the attributes used for grouping
	 * @return a {@code Stream} of {@code Record}s that contain the specified attributes and that are generated using
	 *         the specified tables predicate, and grouping attributes
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions
	 */
	private Operator query(List<String> attributeDescriptions, List<String> tableNames, String predicate,
			List<String> groupingAttributes) throws ParsingException {
		Operator o = new Scan(tables.get(tableNames.get(0)));
		for (int i = 1; i < tableNames.size(); i++)
			o = new NaturalJoin(o, tables.get(tableNames.get(i)));
		if (predicate != null)
			o = new Selection(o, predicate);
		if (groupingAttributes != null)
			return new Aggregation(o, groupingAttributes, aggregationDescriptions(attributeDescriptions));
		if (this.hasAggregateFunctions(attributeDescriptions))
			return new Aggregation(o, List.of(), aggregationDescriptions(attributeDescriptions));
		if (attributeDescriptions == null
				|| attributeDescriptions.size() == 1 && attributeDescriptions.get(0).equals("*"))
			return o;
		return new Projection(o, attributeDefinitions(attributeDescriptions));
	}

	/**
	 * Converts the specified {@code String} descriptions into a {@code Map} containing entries each with a description
	 * of the aggregate function to apply and the corresponding output attribute name.
	 * 
	 * @param descriptions
	 *            {@code String} descriptions explaining the aggregate function to apply and the corresponding output
	 *            attribute name
	 * @return a {@code Map} containing entries each with a description of the aggregate function to apply and the
	 *         corresponding output attribute name
	 */

	static Map<String, String> aggregationDescriptions(List<String> descriptions) {
		var m = new LinkedHashMap<String, String>();
		for (var description : descriptions) {
			try {
				var tokens = description.split(" as ");
				m.put(tokens[0].trim(), tokens[1].trim());
			} catch (Exception e) {
			}
		}
		return m;
	}

	/**
	 * Constructs a {@code Map} containing attribute names and the {@code ArithmeticExpression}s for getting the values
	 * of the attributes
	 * 
	 * @param descriptions
	 *            {@code String} descriptions of the attribute names and the {@code ArithmeticExpression}s for them
	 * @return a {@code Map} containing attribute names and the {@code ArithmeticExpression}s for getting the values of
	 *         the attributes
	 * @throws ParsingException
	 *             if a parsing error occurs
	 */
	private static Map<String, ArithmeticExpression> attributeDefinitions(List<String> descriptions)
			throws ParsingException {
		var attributeDefinitions = new LinkedHashMap<String, ArithmeticExpression>();
		for (String description : descriptions) {
			var tokens = description.split(" as ");
			var attributeName = tokens.length == 2 ? tokens[1].trim() : description.trim();
			var expression = tokens.length == 2 ? new ArithmeticExpression(tokens[0])
					: new ArithmeticExpression(description);
			try { // for each attribute definition, add an attribute to the output schema
				attributeDefinitions.put(attributeName, expression);
			} catch (Exception e) {
				throw new ParsingException();
			}
		}
		return attributeDefinitions;
	}

	/**
	 * Determines whether or not the specified {@code List} of attribute definitions has aggregate functions.
	 * 
	 * @param attributeDefinitions
	 *            a {@code List} of attribute definitions
	 * @return {@code true} if the specified {@code List} of attribute definitions has aggregate functions;
	 *         {@code false} otherwise
	 */
	private boolean hasAggregateFunctions(List<String> attributeDefinitions) {
		for (var a : attributeDefinitions)
			for (var name : Aggregation.aggregateFunctionNames())
				if (a.contains(name + "("))
					return true;
		return false;
	}

}
