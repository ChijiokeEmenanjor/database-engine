package db.expression;

import java.util.LinkedList;

/**
 * A {@code StringTokenizer} parses a string into tokens.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class StringTokenizer implements java.util.Iterator<String> {

	/**
	 * A string tokenizer.
	 */
	java.util.StringTokenizer tokenizer;

	/**
	 * The next token.
	 */
	private String next = null;

	/**
	 * The current token.
	 */
	private String current = null;

	/**
	 * Constructs a {@code StringTokenizer}.
	 * 
	 * @param str
	 *            a string to be parsed
	 * @param delimiters
	 *            the delimiters
	 * @param stringDelim
	 *            the delimiter for representing the beginning and end of each string object
	 * @param commentDelim
	 *            the delimiter for representing the beginning of each comment
	 */
	public StringTokenizer(String str, String delimiters, char stringDelim, char commentDelim) {
		tokenizer = new java.util.StringTokenizer(str, delimiters + stringDelim + commentDelim + " ", true);
		prepare();
	}

	/**
	 * Prepares the next token.
	 */
	protected void prepare() {
		current = next;
		while (true) {
			if (!tokenizer.hasMoreTokens()) { // if no tokens left
				next = null;
				return;
			}
			String token = tokenizer.nextToken();
			if (token.equals("\"")) { // beginning of a String.
				next = "";
				while ((token = tokenizer.nextToken()) != null && !token.equals("\"")) {
					next += token;
				}
				next = "\"" + next + "\"";
				return;
			}
			if (token.equals("#")) { // beginning of the comment
				next = null;
				tokenizer = new java.util.StringTokenizer("");
				return;
			} else if (!token.equals(" ")) { // skip the blanks
				next = token;
				return;
			}
		}
	}

	/**
	 * Returns {@code true} if this {@code StringTokenizer} has a next token; {@code false} otherwise.
	 */
	@Override
	public boolean hasNext() {
		return next != null;
	}

	/**
	 * Returns the next token; {@code null} if there is no such token.
	 */
	@Override
	public String next() {
		String r = next;
		prepare();
		return r;
	}

	/**
	 * Returns the remaining tokens.
	 * 
	 * @return the remaining tokens
	 */
	public String[] remainder() {
		LinkedList<String> remainder = new LinkedList<String>();
		while (hasNext()) {
			remainder.add(next());
		}
		return remainder.toArray(new String[] {});
	}

	/**
	 * Returns the current token.
	 * 
	 * @return the current token
	 */
	public String currentToken() {
		return current;
	}

	/**
	 * Determines whether or not the current token is the same as the specified string.
	 * 
	 * @param str
	 *            a string to compare with the current token
	 * @return {@code true} if the current token is the same as the specified string; {@code false} otherwise
	 */
	public boolean isCurrentToken(String str) {
		return (str.equals(current));
	}

	/**
	 * Determines whether or not the current token represents a number.
	 * 
	 * @return {@code true} if the current token represents a number; {@code false} otherwise
	 */

	public boolean isCurrentTokenNumeric() {
		try {
			str2Number(current);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Determines whether the current token is a string or not.
	 * 
	 * @return {@code true} if the current token is a string; {@code false} otherwise
	 */
	public boolean isCurrentTokenString() {
		String t = current.trim();
		return t.indexOf("\"") == 0 && t.lastIndexOf("\"") == t.length() - 1;
	}

	/**
	 * Returns the number represented by the specified string.
	 * 
	 * @param str
	 *            a string
	 * @return the number represented by the specified string
	 * @throws NumberFormatException
	 *             if the specified string does not represent a number
	 */
	public static Number str2Number(String str) throws NumberFormatException {
		try {
			return Integer.parseInt(str);
		} catch (Exception e) {
			return Double.parseDouble(str);
		}
	}

}
