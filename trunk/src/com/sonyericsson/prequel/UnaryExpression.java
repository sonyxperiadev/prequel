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

class UnaryExpression extends Expression {

    public static final int NOP = 0;

    public static final int NOT = 1;

    public static final int NEGATE = 2;

    public static final int NOT_NULL = 3;

    public static final int IS_NULL = 4;

    private int operator;

    private Expression right;

    public UnaryExpression(int operator, Expression right) {
	this.operator = operator;
	this.right = right;
    }

    public Object evaluate(Table source, int row) {
	Object rValue = right.evaluate(source, row);
	switch (operator) {
	case NOP:
	    return rValue;
	case NOT:
	    return !((Boolean) rValue);
	case NEGATE:
	    return -((Integer) rValue);
	case NOT_NULL:
	    return rValue != null;
	case IS_NULL:
	    return rValue == null;
	default:
	    throw new IllegalStateException("Invalid operator: " + operator);
	}
    }

    @Override
    public String toString() {
	switch (operator) {
	case NOT:
	    return "NOT (" + right.toString() + ")";
	case NEGATE:
	    return "-(" + right.toString() + ")";
	case NOT_NULL:
	    return "(" + right.toString() + ") NOT NULL";
	case NOP:
	    return "(" + right.toString() + ")";
	case IS_NULL:
	    return "(" + right.toString() + ") ISNULL";
	default:
	    return "";
	}
    }

    @Override
    public int type(Table source) {
	return 0;
    }

}
