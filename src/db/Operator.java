package db;

import java.util.stream.Stream;

/**
 * An {@code Operator} processes {@code Record}s and produces {Record}s.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public abstract class Operator {

	/**
	 * Returns the output schema of this {@code Operator}.
	 * 
	 * @return the output schema of this {@code Operator}
	 */
	public abstract TableSchema outputSchema();

	/**
	 * Returns the output {@code Stream<Record>} of this {@code Operator}.
	 * 
	 * @return the output {@code Stream<Record>} of this {@code Operator}
	 */
	public abstract Stream<Record> stream();

}
