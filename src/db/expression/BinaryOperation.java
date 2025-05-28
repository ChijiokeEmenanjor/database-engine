package db.expression;

import java.io.PrintStream;

/**
 * A {@code BinaryOperation} represents a binary operation.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public abstract class BinaryOperation extends Node {

	/**
	 * The left child.
	 */
	protected Node left;

	/**
	 * The right child.
	 */
	protected Node right;

	/**
	 * Constructs a {@code BinaryOperation}.
	 * 
	 * @param left
	 *            the left child
	 * @param right
	 *            the right child
	 */
	public BinaryOperation(Node left, Node right) {
		this.left = left;
		this.right = right;
	}

	/**
	 * Prints the sub-tree rooted at this {@code BinaryOperation}.
	 * 
	 * @param out
	 *            a {@code PrintStream}
	 * @param indentation
	 *            the indentation
	 */
	@Override
	protected void print(PrintStream out, int indentation) {
		super.print(out, indentation);
		left.print(out, indentation + 1);
		right.print(out, indentation + 1);
	}

}
