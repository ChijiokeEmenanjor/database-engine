package db.expression;

/**
 * An {@code EqualTo} represents an equality operation.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class EqualTo extends BinaryLogicalOperation {

	/**
	 * Constructs an {@code EqualTo}.
	 * 
	 * @param left
	 *            the left child
	 * @param right
	 *            the right child
	 */
	public EqualTo(Node left, Node right) {
		super(left, right);
	}

	/**
	 * Evaluates the expression represented by this {@code EqualTo} and its descendants.
	 * 
	 * @return the result of evaluating the expression represented by this {@code EqualTo} and its descendants
	 */
	@Override
	public Object evaluate() {
		Object l = left.evaluate();
		Object r = right.evaluate();
		if (l != null && l instanceof String)
			return l.equals(r);
		l = object2num(l);
		r = object2num(r);
		if (l instanceof Integer && r instanceof Integer)
			return ((Integer) l).intValue() == ((Integer) r).intValue();
		else if (l instanceof Integer && r instanceof Double)
			return ((Integer) l).intValue() == ((Double) r).doubleValue();
		else if (l instanceof Double && r instanceof Integer)
			return ((Double) l).doubleValue() == ((Integer) r).intValue();
		else if (l instanceof Double && r instanceof Double)
			return ((Double) l).doubleValue() == ((Double) r).doubleValue();
		else
			throw new UnsupportedOperationException();
	}

}
