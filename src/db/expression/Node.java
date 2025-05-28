package db.expression;

import java.io.PrintStream;

/**
 * A {@code Node} represents a node in a parse tree.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public abstract class Node {

	/**
	 * Evaluates the expression represented by this {@code Node} and its descendants.
	 * 
	 * @return the result of evaluating the expression represented by this {@code Node} and its descendants
	 */
	public abstract Object evaluate();

	/**
	 * Prints the sub-tree rooted at this {@code Node}.
	 * 
	 * @param out
	 *            a {@code PrintStream}
	 * @param indentation
	 *            the indentation
	 */
	protected void print(PrintStream out, int indentation) {
		if (indentation > 0)
			out.println(String.format("%" + indentation + "s%s", "", toString()));
		else
			out.println(toString());
	}

	/**
	 * Returns a string representation of this {@code Node}.
	 */
	@Override
	public String toString() {
		return getClass().getCanonicalName();
	}

	/**
	 * Return a number that corresponds to the specified object.
	 * 
	 * @param o
	 *            an object
	 * @return a number that corresponds to the specified object
	 * @throws NumberFormatException
	 *             if the specified string does not represent a number
	 */
	protected static Object object2num(Object o) throws NumberFormatException {
		if (o instanceof Integer || o instanceof Double)
			return o;
		return StringTokenizer.str2Number(o.toString());
	}

}
