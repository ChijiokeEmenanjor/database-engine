package db.expression;

/**
 * An {@code ArithmeticExpression} represents an arithmetic expression.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class ArithmeticExpression extends Expression {

	/**
	 * Constructs an {@code ArithmeticExpression}.
	 * 
	 * @param expression
	 *            a string representing an algebraic expression
	 * @throws ParsingException
	 *             if a parsing error occurs
	 */
	public ArithmeticExpression(String expression) throws ParsingException {
		StringTokenizer tokenizer = new StringTokenizer(expression, plus + minus + times + slash + lparen + rparen,
				'\"', '#');
		tokenizer.next();
		root = expression(tokenizer);
	}

}
