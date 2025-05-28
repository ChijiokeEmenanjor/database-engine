package db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import db.expression.ArithmeticExpression;
import db.expression.ParsingException;

/**
 * A {@code Projection} converts each input {@code Record} into an output {@code Record}.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Projection extends UnaryOperator {

	/**
	 * The {@code ExpressionEvaluator}s for this {@code Projection}.
	 */
	protected List<ExpressionEvaluator> evaluators = new ArrayList<ExpressionEvaluator>();

	/**
	 * The output schema of this {@code Projection}.
	 */
	protected TableSchema outputSchema = new TableSchema();

	/**
	 * Constructs a {@code Projection}.
	 * 
	 * @param input
	 *            the input {@code Operator} for the {@code Projection}
	 * @param attributeDefinitions
	 *            strings representing expressions that define the attributes to include the output schema of this
	 *            {@code Projection}
	 * @throws ParsingException
	 *             if an error occurs while parsing the expressions.
	 */
	public Projection(Operator input, Map<String, ArithmeticExpression> attributeDefinitions) throws ParsingException {
		super(input);
		for (var attributeDefinition : attributeDefinitions.entrySet()) {
			outputSchema.attribute(attributeDefinition.getKey());
			evaluators.add(new ExpressionEvaluator(attributeDefinition.getValue(), inputSchema()));
		}
	}

	/**
	 * Returns the output {@code TableSchema} of this {@code Projection}.
	 * 
	 * @return the output {@code TableSchema} of this {@code Projection}
	 */
	@Override
	public TableSchema outputSchema() {
		return outputSchema;
	}

	/**
	 * Returns the output {@code Stream<Record>} of this {@code Projection}.
	 * 
	 * @return the output {@code Stream<Record>} of this {@code Projection}
	 */
	@Override
	public Stream<Record> stream() {
		return input.stream().map(r -> outputRecord(r));
	}

	/**
	 * Constructs an output {@code Record} using the specified input {@code Record}.
	 * 
	 * @param r
	 *            an input {@code Record}
	 * @return the output {@code Record} generated from the specified input {@code Record}
	 */
	private Record outputRecord(Record r) {
		Object[] attributValues = new Object[outputSchema.size()]; // an array to contain the attribute values
		for (int i = 0; i < attributValues.length; i++) // for each output attribute
			attributValues[i] = evaluators.get(i).evaluate(r); // get the attribute value from the evaluator
		return new Record(outputSchema, attributValues); // construct a record containing the attribute values
	}

}
