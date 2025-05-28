package db.expression;

/**
 * A {@code Constant} represents a constant.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
class Constant extends LeafNode {

	/**
	 * Constructs a {@code Constant}.
	 * 
	 * @param val
	 *            a value
	 */
	Constant(Object val) {
		super(val);
	}

	/**
	 * Returns the value of this {@code Constant}.
	 * 
	 * @return the value of this {@code Constant}
	 */
	@Override
	public Object evaluate() {
		return val;
	}

}
