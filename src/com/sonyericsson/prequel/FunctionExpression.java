/* Prequel � A set of simple database APIs in Java SE, similar to SQLite
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
 *   P�r Spjuth (par.spjuth@sonyericsson.com)
 */
package com.sonyericsson.prequel;

import java.util.Vector;

class FunctionExpression extends Expression {

    private String func;

    private Vector<Expression> params;

    public FunctionExpression(String func, Vector<Expression> params) {
	this.func = func;
	this.params = params;
    }

    public Object evaluate(Table source, int row) {
	if (func.equals("COALESCE")) {
	    for (Expression p : params) {
		Object result = p.evaluate(source, row);
		if (result != null) {
		    return result;
		}
	    }
	    return null;
	} else {
	    throw new IllegalArgumentException("Unknown SQL-function: " + func);
	}
    }

    @Override
    public String toString() {
	StringBuilder b = new StringBuilder(func);
	String prefix = "(";
	for (Expression p : params) {
	    b.append(prefix);
	    b.append(p.toString());
	    prefix = ", ";
	}
	b.append(")");
	return b.toString();
    }

    @Override
    public int type(Table source) {
	throw new IllegalStateException("Not implemented");
    }

}
