package db.expression;

/**
 * A {@code Variable} represents a variable in an expression.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Variable extends LeafNode {

	/**
	 * The name of this {@code Variable}.
	 */
	protected String name;

	/**
	 * Constructs a {@code Variable}.
	 * 
	 * @param name
	 *            the name of the {@code Variable}
	 */
	Variable(String name) {
		super(null);
		this.name = name;
	}

	/**
	 * Returns the name of this {@code Variable}.
	 * 
	 * @return the name of this {@code Variable}
	 */
	public String name() {
		return name;
	}

	/**
	 * Sets the value of this {@code LeafNode}.
	 * 
	 * @param val
	 *            the value
	 */
	public void setValue(Object val) {
		this.val = val;
	}

	/**
	 * Returns the value of this {@code Variable}.
	 * 
	 * @return the value of this {@code Variable}
	 */
	@Override
	public Object evaluate(){
		if (val == null)
			throw new UnboundVariableException(name);
		return val;
	}

	/**
	 * Returns a string representation of this {@code Variable}.
	 */
	@Override
	public String toString() {
		return name + "=" + val;
	}

}