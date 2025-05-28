package db.expression;

/**
 * A {@code GreaterThan} represents a binary operation that returns {@code true} if the left operand is greater than the
 * right operand.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class GreaterThan extends BinaryLogicalOperation {

	/**
	 * Constructs a {@code GreaterThan}.
	 * 
	 * @param left
	 *            the left child
	 * @param right
	 *            the right child
	 */
	public GreaterThan(Node left, Node right) {
		super(left, right);
	}

	/**
	 * Evaluates the expression represented by this {@code GreaterThan} and its descendants.
	 * 
	 * @return the result of evaluating the expression represented by this {@code GreaterThan} and its descendants
	 */
	@Override
	public Object evaluate() {
		Object l = object2num(left.evaluate());
		Object r = object2num(right.evaluate());
		if (l instanceof Integer && r instanceof Integer)
			return ((Integer) l).intValue() > ((Integer) r).intValue();
		else if (l instanceof Integer && r instanceof Double)
			return ((Integer) l).intValue() > ((Double) r).doubleValue();
		else if (l instanceof Double && r instanceof Integer)
			return ((Double) l).doubleValue() > ((Integer) r).intValue();
		else if (l instanceof Double && r instanceof Double)
			return ((Double) l).doubleValue() > ((Double) r).doubleValue();
		else
			throw new UnsupportedOperationException();
	}

}
