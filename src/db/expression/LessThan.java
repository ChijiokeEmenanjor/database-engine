package db.expression;

/**
 * A {@code LessThan} represents a binary operation that returns {@code true} if the left operand is less than the right
 * operand.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class LessThan extends BinaryLogicalOperation {

	/**
	 * Constructs a {@code LessThan}.
	 * 
	 * @param left
	 *            the left child
	 * @param right
	 *            the right child
	 */
	public LessThan(Node left, Node right) {
		super(left, right);
	}

	/**
	 * Evaluates the expression represented by this {@code LessThan} and its descendants.
	 * 
	 * @return the result of evaluating the expression represented by this {@code LessThan} and its descendants
	 */
	@Override
	public Object evaluate() {
		Object l = object2num(left.evaluate());
		Object r = object2num(right.evaluate());
		if (l instanceof Integer && r instanceof Integer)
			return ((Integer) l).intValue() < ((Integer) r).intValue();
		else if (l instanceof Integer && r instanceof Double)
			return ((Integer) l).intValue() < ((Double) r).doubleValue();
		else if (l instanceof Double && r instanceof Integer)
			return ((Double) l).doubleValue() < ((Integer) r).intValue();
		else if (l instanceof Double && r instanceof Double)
			return ((Double) l).doubleValue() < ((Double) r).doubleValue();
		else
			throw new UnsupportedOperationException();
	}

}
