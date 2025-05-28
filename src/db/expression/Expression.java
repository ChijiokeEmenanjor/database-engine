package db.expression;

import java.io.PrintStream;
import java.util.HashMap;

/**
 * An {@code Expression} represents an expression.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Expression {

	/**
	 * The plus symbol.
	 */
	public static String plus = "+";

	/**
	 * The minus symbol.
	 */
	public static String minus = "-";

	/**
	 * The times symbol.
	 */
	public static String times = "*";

	/**
	 * The slash symbol.
	 */
	public static String slash = "/";

	/**
	 * The left parenthesis.
	 */
	public static String lparen = "(";

	/**
	 * The right parenthesis.
	 */
	public static String rparen = ")";

	/**
	 * The equality symbol.
	 */
	public static String eq = "=";

	/**
	 * The "less than" symbol.
	 */
	public static String lt = "<";

	/**
	 * The "greater than" symbol.
	 */
	public static String gt = ">";

	/**
	 * The variables contained in this {@code Expression}.
	 */
	protected HashMap<String, Variable> variables = new HashMap<String, Variable>();

	/**
	 * The root note of the parse tree.
	 */
	protected Node root;

	/**
	 * Returns the {@code Variable} specified by the given name; {@code null} if no such {@code Variable}.
	 * 
	 * @param name
	 *            the name of the {@code Variable}.
	 * @return the {@code Variable} specified by the given name; {@code null} if no such {@code Variable}
	 */
	public Variable variable(String name) {
		return variables.get(name);
	}

	/**
	 * Sets the value of the specified variable.
	 * 
	 * @param name
	 *            the name of the variable
	 * @param val
	 *            the value
	 * @throws UnregisteredVariableException
	 *             if an unregistered variable is referenced
	 */
	public void setVariable(String name, Object val) throws UnregisteredVariableException {
		Variable v = variables.get(name);
		if (v == null)
			throw new UnregisteredVariableException();
		v.setValue(val);
	}

	/**
	 * Evaluates this {@code Expression}.
	 * 
	 * @return the result of evaluating this {@code Expression}
	 */
	public Object evaluate() {
		return root.evaluate();
	}

	/**
	 * Returns the variables contained in this {@code Expression}.
	 * 
	 * @return the variables contained in this {@code Expression}
	 */
	public Iterable<Variable> variables() {
		return variables.values();
	}

	/**
	 * Prints this {@code Expression}.
	 * 
	 * @param out
	 *            a {@code PrintStream}
	 */
	public void print(PrintStream out) {
		root.print(out, 0);
	}

	/**
	 * Parses the condition from the specified {@code StringTokenizer}.
	 * 
	 * @param tokenizer
	 *            a {@code StringTokenizer}
	 * @return a parse tree representing an expression
	 * @throws ParsingException
	 *             if an error occurs while parsing an expression
	 */
	protected Node condition(StringTokenizer tokenizer) throws ParsingException {
		Node node = expression(tokenizer);
		if (tokenizer.isCurrentToken(eq)) {
			tokenizer.next();
			return new EqualTo(node, expression(tokenizer));
		} else if (tokenizer.isCurrentToken(lt)) {
			tokenizer.next();
			return new LessThan(node, expression(tokenizer));
		} else if (tokenizer.isCurrentToken(gt)) {
			tokenizer.next();
			return new GreaterThan(node, expression(tokenizer));
		} else
			throw new ParsingException();
	}

	/**
	 * Parses the expression from the specified {@code StringTokenizer}.
	 * 
	 * @param tokenizer
	 *            a {@code StringTokenizer}
	 * @return a parse tree representing an expression
	 * @throws ParsingException
	 *             if an error occurs while parsing an expression
	 */
	protected Node expression(StringTokenizer tokenizer) throws ParsingException {
		boolean negation = false;
		if (tokenizer.isCurrentToken(plus) || tokenizer.isCurrentToken(minus)) {
			if (tokenizer.isCurrentToken(minus))
				negation = true;
			tokenizer.next();
		}
		Node node = negation ? new Negation(term(tokenizer)) : term(tokenizer);
		while (tokenizer.isCurrentToken(plus) || tokenizer.isCurrentToken(minus)) {
			String operation = tokenizer.currentToken();
			tokenizer.next();
			node = operation.equals(plus) ? new Addition(node, term(tokenizer))
					: new Subtraction(node, term(tokenizer));
		}
		return node;
	}

	/**
	 * Parses the term from the specified {@code StringTokenizer}.
	 * 
	 * @param tokenizer
	 *            a {@code StringTokenizer}
	 * @return a parse tree representing a term
	 * @throws ParsingException
	 *             if an error occurs while parsing a term
	 */
	protected Node term(StringTokenizer tokenizer) throws ParsingException {
		Node node = factor(tokenizer);
		while (tokenizer.isCurrentToken(times) || tokenizer.isCurrentToken(slash)) {
			String operation = tokenizer.currentToken();
			tokenizer.next();
			node = operation.equals(times) ? new Multiplication(node, factor(tokenizer))
					: new Division(node, factor(tokenizer));
		}
		return node;
	}

	/**
	 * Parses the factor from the specified {@code StringTokenizer}.
	 * 
	 * @param tokenizer
	 *            a {@code StringTokenizer}
	 * @return a parse tree representing a factor
	 * @throws ParsingException
	 *             if an error occurs while parsing a factor
	 */
	protected Node factor(StringTokenizer tokenizer) throws ParsingException {
		if (tokenizer.isCurrentToken(lparen)) {
			tokenizer.next();
			Node node = expression(tokenizer);
			if (!tokenizer.isCurrentToken(rparen))
				throw new ParsingException();
			tokenizer.next();
			return node;
		} else if (tokenizer.isCurrentTokenNumeric()) {
			LeafNode c = new Constant(StringTokenizer.str2Number(tokenizer.currentToken()));
			tokenizer.next();
			return c;
		} else if (tokenizer.isCurrentTokenString()) {
			String s = tokenizer.currentToken();
			s = s.replaceAll("\"", "");
			LeafNode c = new Constant(s);
			tokenizer.next();
			return c;
		} else {
			String name = tokenizer.currentToken();
			Variable v = variables.get(name);
			if (v == null) {
				v = new Variable(name);
				variables.put(v.name(), v);
			}
			tokenizer.next();
			return v;
		}
	}

}
