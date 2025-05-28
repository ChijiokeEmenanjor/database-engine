package db.expression;

/**
 * A {@code LogicalExpression} represents a logical expression.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class LogicalExpression extends Expression {

	/**
	 * Constructs a {@code LogicalExpression}.
	 * 
	 * @param expression
	 *            a string representing a logical expression.
	 * @throws ParsingException
	 *             if a parsing error occurs
	 */
	public LogicalExpression(String expression) throws ParsingException {
		StringTokenizer tokenizer = new StringTokenizer(expression, plus + minus + times + slash + lparen + rparen + eq,
				'\"', '#');
		tokenizer.next();
		root = condition(tokenizer);
	}

}
