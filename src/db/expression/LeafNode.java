package db.expression;

/**
 * A {@code LeafNode} contains a value.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
abstract class LeafNode extends Node {

	/**
	 * The value of this {@code LeafNode}.
	 */
	protected Object val;

	/**
	 * Constructs a {@code LeafNode}.
	 * 
	 * @param val
	 *            a value
	 */
	LeafNode(Object val) {
		this.val = val;
	}

	/**
	 * Returns a string representation of this {@code LeafNode}.
	 */
	@Override
	public String toString() {
		return val.toString();
	}

}
