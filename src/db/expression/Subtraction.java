package db.expression;

/**
 * A {@code Subtraction} represents a subtraction operation.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
class Subtraction extends BinaryNumericOperation {

	/**
	 * Constructs a {@code Subtraction}.
	 * 
	 * @param left
	 *            the left child
	 * @param right
	 *            the right child
	 */
	public Subtraction(Node left, Node right) {
		super(left, right);
	}

	/**
	 * Evaluates the expression represented by this {@code Subtraction} and its descendants.
	 * 
	 * @return the result of evaluating the expression represented by this {@code Subtraction} and its descendants
	 */
	@Override
	public Object evaluate(){
		Object l = object2num(left.evaluate());
		Object r = object2num(right.evaluate());
		if (l instanceof Integer && r instanceof Integer)
			return ((Integer) l).intValue() - ((Integer) r).intValue();
		else if (l instanceof Integer && r instanceof Double)
			return ((Integer) l).intValue() - ((Double) r).doubleValue();
		else if (l instanceof Double && r instanceof Integer)
			return ((Double) l).doubleValue() - ((Integer) r).intValue();
		else if (l instanceof Double && r instanceof Double)
			return ((Double) l).doubleValue() - ((Double) r).doubleValue();
		else
			throw new UnsupportedOperationException();
	}

}
