/* Prequel – A set of simple database APIs in Java SE, similar to SQLite
 * Copyright (c) 2011 Sony Ericsson Mobile Communications AB
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * Authors: 
 *   Pär Spjuth (par.spjuth@sonyericsson.com)
 */
package com.sonyericsson.prequel;

class Tokenizer {

    private static final char EOL = (char) -1;

    private String data;

    private int pos;

    private String current;

    private int currentPos;

    private String next;

    private char nextChar() {
	pos++;
	if (pos == data.length()) {
	    return EOL;
	} else {
	    return data.charAt(pos);
	}
    }

    private char currentChar() {
	if (pos == data.length()) {
	    return EOL;
	} else {
	    return data.charAt(pos);
	}
    }

    // private void putBack() {
    // pos--;
    // }

    public Tokenizer(String source) {
	// System.out.println("SOURCE: " + source);
	data = source;
	pos = -1;
	current = null;
	while (Character.isWhitespace(nextChar())) {
	}
    }

    public void next() {
	if (next != null) {
	    current = next;
	    next = null;
	} else {
	    int startPos = pos;
	    if (currentChar() == EOL) {
		current = null;
	    } else {
		if (",;()?:@$|&*%+-".indexOf(currentChar()) != -1) {
		    nextChar();
		} else if (currentChar() == '!' || currentChar() == '='
			|| currentChar() == '>' || currentChar() == '<') {
		    if (nextChar() == '=') {
			nextChar();
		    }
		} else if (currentChar() == '\'') {
		    while (nextChar() != '\'') {
		    }
		    nextChar();
		} else if (currentChar() == '\"') {
		    while (nextChar() != '\"') {
		    }
		    nextChar();
		} else if (Character.isJavaIdentifierStart(currentChar())) {
		    // TODO: Should refactor parser so that it can handle "." to
		    // separate database from table!
		    while (Character.isJavaIdentifierPart(nextChar())
			    || currentChar() == '.') {
		    }
		} else if (Character.isDigit(currentChar())) {
		    while (Character.isDigit(nextChar())) {
		    }
		} else {
		    throw new IllegalArgumentException("Unexpected character '"
			    + currentChar() + "' at " + pos);
		}
		current = data.substring(startPos, pos);
		if (Character.isWhitespace(currentChar())) {
		    while (Character.isWhitespace(nextChar())) {
		    }
		}
	    }
	    currentPos = startPos;
	}
	// System.out.println("Token: '" + current + "'");
    }

    public String current() {
	return current;
    }

    public boolean currentIsString() {
	return current.charAt(0) == '\'';
    }

    public boolean currentIsNumber() {
	return Character.isDigit(current.charAt(0));
    }

    public boolean currentIsIdentifier() {
	return Character.isJavaIdentifierStart(current.charAt(0));
    }

    public void currentAsNext() {
	next = current;
    }

    public int getPos() {
	if (next != null) {
	    return currentPos;
	} else {
	    return pos;
	}
    }

    @Override
    public String toString() {
	StringBuilder res = new StringBuilder(data + "\n");
	for (int i = 0; i < pos; i++) {
	    res.append(' ');
	}
	res.append("^\n");
	res.append("current: " + current + "\n");
	res.append("next: " + next + "\n");
	return res.toString();
    }

}
