package db;

import java.util.LinkedList;

import db.expression.Expression;
import db.expression.UnboundVariableException;
import db.expression.Variable;

/**
 * An {@code ExpressionEvaluator} can evaluate an {@code Expression} on each given {@code Record}.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class ExpressionEvaluator {

	/**
	 * The {@code Expression} of this {@code ExpressionEvaluator}.
	 */
	Expression expression;

	/**
	 * The indices of the attributes that correspond to the {@code Variable}s of the {@code ArithmeticExpression}.
	 */
	Integer[] indices;

	/**
	 * Constructs an {@code ExpressionEvaluator}.
	 * 
	 * @param expression
	 *            an {@code Expression}
	 * @param schema
	 *            a {@code TableSchema}
	 * @throws UnboundVariableException
	 *             if a variable in the {@code Expression} does not correspond to any attribute in the
	 *             {@code TableSchema}
	 */
	public ExpressionEvaluator(Expression expression, TableSchema schema) throws UnboundVariableException {
		this.expression = expression;
		LinkedList<Integer> indices = new LinkedList<Integer>();
		for (Variable v : expression.variables()) {
			Integer i = schema.attributeIndex(v.name());
			if (i == null)
				throw new UnboundVariableException(v.name());
			indices.add(i);
		}
		this.indices = indices.toArray(new Integer[0]);
	}

	/**
	 * Evaluates the {@code Expression} of this {@code ExpressionEvaluator} on the specified {@code Record}.
	 * 
	 * @param r
	 *            a {@code Record}
	 * @return the result of evaluating the {@code Expression} on the specified {@code Record}
	 */
	public Object evaluate(Record r) {
		int i = 0;
		for (Variable v : expression.variables())
			v.setValue(r.value(indices[i++]));
		return expression.evaluate();
	}

}
