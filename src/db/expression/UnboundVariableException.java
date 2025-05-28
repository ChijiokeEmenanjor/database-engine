package db.expression;

/**
 * An {@code UnboundVariableException} is thrown if an expression being evaluated has a variable whose type/value is not
 * set.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class UnboundVariableException extends RuntimeException {

	/**
	 * Automatically generated serial version UID.
	 */
	private static final long serialVersionUID = 2293708101502204882L;

	/**
	 * Constructs an {@code UnboundVariableException}.
	 * 
	 * @param name
	 *            the name of the variable whose type/value is not set
	 */
	public UnboundVariableException(String name) {
		super(name);
	}

}
