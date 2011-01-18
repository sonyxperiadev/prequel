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

import java.util.Arrays;

class BinaryExpression extends Expression {

    public static final int CONCAT = 0;
    public static final int EQUALS = 1;
    public static final int NOT_EQUALS = 2;
    public static final int AND = 3;
    public static final int OR = 4;
    public static final int LESSER = 5;
    public static final int LESSER_EQUALS = 6;
    public static final int GREATER = 7;
    public static final int GREATER_EQUALS = 8;
    public static final int ADD = 9;
    public static final int SUBTRACT = 10;
    public static final int MULTIPLY = 11;
    public static final int DIVIDE = 12;
    public static final int MODULUS = 13;
    public static final int SHIFT_LEFT = 14;
    public static final int SHIFT_RIGHT = 15;

    private int operator;

    private Expression left;

    private Expression right;

    private String opToString(int op) {
	switch (operator) {
	case CONCAT:
	    return "||";
	case EQUALS:
	    return "==";
	case NOT_EQUALS:
	    return "!=";
	case AND:
	    return "AND";
	case OR:
	    return "OR";
	case LESSER:
	    return "<";
	case LESSER_EQUALS:
	    return "<=";
	case GREATER:
	    return ">";
	case GREATER_EQUALS:
	    return ">=";
	default:
	    return "???";
	}
    }

    private double forceDouble(Object o) {
	if (o instanceof Integer) {
	    return (Integer) o;
	} else if (o instanceof Long) {
	    return (Long) o;
	} else if (o instanceof Double) {
	    return (Double) o;
	} else if (o instanceof String) {
	    return Double.valueOf((String) o);
	} else {
	    return 0;
	}
    }

    private boolean forceBool(Object o) {
	if (o instanceof Integer) {
	    return ((Integer) o) == 1;
	} else if (o instanceof Long) {
	    return ((Long) o) == 1;
	} else if (o instanceof Double) {
	    return ((Double) o) == 1;
	} else if (o instanceof String) {
	    String s = (String) o;
	    if (s.equalsIgnoreCase("TRUE")) {
		return true;
	    } else if (s.equalsIgnoreCase("FALSE")) {
		return false;
	    } else {
		try {
		    if (Double.valueOf(s) == 1) {
			return true;
		    }
		} catch (NumberFormatException e) {
		    return false;
		}
		return false;
	    }
	} else {
	    return false;
	}
    }

    private boolean isLesser(Object lValue, Object rValue) {
	if (lValue == null) {
	    return true;
	} else if (rValue == null) {
	    return false;
	} else if ((lValue instanceof Double || lValue instanceof Long || lValue instanceof Integer)
		&& (rValue instanceof String || rValue instanceof byte[])) {
	    return true;
	} else if (lValue instanceof String && rValue instanceof byte[]) {
	    return true;
	} else if (lValue instanceof String && rValue instanceof String) {
	    return ((String) lValue).compareTo((String) rValue) < 0;
	} else if (lValue instanceof byte[] && rValue instanceof byte[]) {
	    byte[] a = (byte[]) lValue;
	    byte[] b = (byte[]) rValue;
	    if (a.length < b.length) {
		return true;
	    } else if (a.length > b.length) {
		return false;
	    } else {
		for (int i = 0; i < b.length; i++) {
		    if (a[i] < b[i]) {
			return true;
		    } else if (a[i] > b[i]) {
			return false;
		    }
		}
		return false;
	    }
	} else {
	    return forceDouble(lValue) < forceDouble(rValue);
	}
    }

    private boolean isGreater(Object lValue, Object rValue) {
	if (lValue == null) {
	    return false;
	} else if (rValue == null) {
	    return true;
	} else if ((rValue instanceof Double || rValue instanceof Long || rValue instanceof Integer)
		&& (lValue instanceof String || lValue instanceof byte[])) {
	    return true;
	} else if (rValue instanceof String && lValue instanceof byte[]) {
	    return true;
	} else if (lValue instanceof String && rValue instanceof String) {
	    return ((String) rValue).compareTo((String) lValue) > 0;
	} else if (lValue instanceof byte[] && rValue instanceof byte[]) {
	    byte[] a = (byte[]) lValue;
	    byte[] b = (byte[]) rValue;
	    if (a.length > b.length) {
		return true;
	    } else if (a.length < b.length) {
		return false;
	    } else {
		for (int i = 0; i < b.length; i++) {
		    if (a[i] > b[i]) {
			return true;
		    } else if (a[i] < b[i]) {
			return false;
		    }
		}
		return false;
	    }
	} else {
	    return forceDouble(lValue) > forceDouble(rValue);
	}
    }

    private boolean isEqual(Object lValue, Object rValue) {
	if (lValue == null || rValue == null) {
	    return false;
	} else if (lValue.getClass() == rValue.getClass()) {
	    return lValue.equals(rValue);
	} else if ((lValue instanceof Integer || lValue instanceof Long || lValue instanceof Double)
		&& (rValue instanceof Integer || rValue instanceof Long || rValue instanceof Double)) {
	    return forceDouble(lValue) == forceDouble(rValue);
	} else if (lValue instanceof byte[] && rValue instanceof byte[]) {
	    return Arrays.equals((byte[]) lValue, (byte[]) rValue);
	} else {
	    return false;
	}
    }

    public BinaryExpression(Expression left, int operator, Expression right) {
	this.operator = operator;
	this.left = left;
	this.right = right;
    }

    public Object evaluate(Table source, int row) {
	Object lValue = left.evaluate(source, row);
	Object rValue = right.evaluate(source, row);
	switch (operator) {
	case CONCAT:
	    return lValue.toString() + rValue.toString();
	case EQUALS:
	    return isEqual(lValue, rValue) ? 1 : 0;
	case NOT_EQUALS:
	    return (!isEqual(lValue, rValue)) ? 1 : 0;
	case AND:
	    return (forceBool(lValue) && forceBool(rValue)) ? 1 : 0;
	case OR:
	    return (forceBool(lValue) || forceBool(rValue)) ? 1 : 0;
	case LESSER:
	    return isLesser(lValue, rValue) ? 1 : 0;
	case LESSER_EQUALS:
	    return (isLesser(lValue, rValue) || isEqual(lValue, rValue)) ? 1
		    : 0;
	case GREATER:
	    return isGreater(lValue, rValue) ? 1 : 0;
	case GREATER_EQUALS:
	    return (isGreater(lValue, rValue) || isEqual(lValue, rValue)) ? 1
		    : 0;
	default:
	    // TODO
	    throw new IllegalStateException("Unimplemented operator: "
		    + opToString(operator));
	}
    }

    public static int getPrecedence(int operator) {
	switch (operator) {
	case CONCAT:
	    return 8;
	case EQUALS:
	case NOT_EQUALS:
	    return 3;
	case AND:
	    return 1;
	case OR:
	    return 2;
	case LESSER:
	case LESSER_EQUALS:
	case GREATER:
	case GREATER_EQUALS:
	    return 4;
	case ADD:
	case SUBTRACT:
	    return 6;
	case MULTIPLY:
	case DIVIDE:
	case MODULUS:
	    return 7;
	case SHIFT_LEFT:
	case SHIFT_RIGHT:
	    return 5;
	default:
	    return 0;
	}
    }

    @Override
    public int type(Table source) {
	// int lValue = left.type(source);
	// int rValue = right.type(source);
	switch (operator) {
	case CONCAT:
	    return Table.TEXT;
	case EQUALS:
	    return Table.INTEGER;
	case NOT_EQUALS:
	    return Table.INTEGER;
	case AND:
	    return Table.INTEGER;
	case OR:
	    return Table.INTEGER;
	case LESSER:
	    return Table.INTEGER;
	case LESSER_EQUALS:
	    return Table.INTEGER;
	case GREATER:
	    return Table.INTEGER;
	case GREATER_EQUALS:
	    return Table.INTEGER;
	default:
	    // TODO
	    throw new IllegalStateException("Unimplemented operator: "
		    + opToString(operator));
	}
    }

    @Override
    public String toString() {
	return "(" + left.toString() + " " + opToString(operator) + " "
		+ right.toString() + ")";
    }

}
