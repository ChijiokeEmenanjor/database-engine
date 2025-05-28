package db;

import java.util.stream.Stream;

import db.expression.LogicalExpression;
import db.expression.ParsingException;

/**
 * A {@code Selection} outputs, among the input {@code Record}s, those that satisfy a specified predicate.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Selection extends UnaryOperator {

    /**
     * The {@code ExpressionEvaluator} for this {@code Selection}.
     */
    protected ExpressionEvaluator evaluator;

    /**
     * The predicate for this {@code Selection}.
     */
    protected String predicate;

    /**
     * Constructs a {@code Selection}.
     * 
     * @param input the input {@code Operator} for the {@code Selection}
     * @param predicate the predicate for the {@code Selection}
     * @throws ParsingException if an error occurs while parsing the expressions
     */
    public Selection(Operator input, String predicate) throws ParsingException {
        super(input);
        this.predicate = predicate;
        evaluator = new ExpressionEvaluator(new LogicalExpression(predicate), input.outputSchema());
    }

    /**
     * Returns the predicate of this {@code Selection}.
     * 
     * @return the predicate of this {@code Selection}
     */
    public String predicate() {
        return predicate;
    }

    /**
     * Returns the output {@code TableSchema} of this {@code Selection}.
     * 
     * @return the output {@code TableSchema} of this {@code Selection}
     */
    @Override
    public TableSchema outputSchema() {
        return input.outputSchema();
    }

    /**
     * Returns the output {@code Stream<Record>} of this {@code Selection}.
     * 
     * @return the output {@code Stream<Record>} of this {@code Selection}
     */
    @Override
    public Stream<Record> stream() {
      
        Stream<Record> inputStream = input.stream();//input stream is gotten
        return inputStream.filter(record -> {//recors filtered based on evaluator
            try {
                return (boolean) evaluator.evaluate(record);
            } catch (Exception e) {//exceptions are handled
                return false; // if the eval is not good then rec is skipped over
            }
        });
    }
}

