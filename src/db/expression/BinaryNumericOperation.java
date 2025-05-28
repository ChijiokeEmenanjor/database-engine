package db.expression;

/**
 * A {@code BinaryNumericOperation} represents a binary numeric operation.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public abstract class BinaryNumericOperation extends BinaryOperation {

	/**
	 * Constructs a {@code BinaryNumericOperation}.
	 * 
	 * @param left
	 *            the left child
	 * @param right
	 *            the right child
	 */
	public BinaryNumericOperation(Node left, Node right) {
		super(left, right);
	}

}
