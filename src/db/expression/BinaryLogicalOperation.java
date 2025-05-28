package db.expression;

/**
 * A {@code BinaryLogicalOperation} represents a binary logical operation.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public abstract class BinaryLogicalOperation extends BinaryOperation {

	/**
	 * Constructs a {@code BinaryLogicalOperation}.
	 * 
	 * @param left
	 *            the left child
	 * @param right
	 *            the right child
	 */
	public BinaryLogicalOperation(Node left, Node right) {
		super(left, right);
	}

}
