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
package com.sonyericsson.prequel.test;

import java.util.Stack;
import java.util.Vector;

import junit.framework.TestCase;

import com.sonyericsson.prequel.Database;
import com.sonyericsson.prequel.InvalidSqlQueryException;

public class TestFreeForAll extends TestCase {

    private Database d;

    private abstract static class Step {

	private Vector<Step> next;

	private void generate(Stack<Step> path, Vector<String> result) {
	    path.add(this);
	    if (next.isEmpty()) {
		StringBuilder sb = new StringBuilder();
		for (Step step : path) {
		    sb.append(step.out());
		}
		result.add(sb.toString());
	    } else {
		for (Step step : next) {
		    step.generate(path, result);
		}
	    }
	    path.pop();
	}

	protected abstract String out();

	public Step() {
	    next = new Vector<Step>();
	}

	public Step add(Step next) {
	    this.next.add(next);
	    return next;
	}

	public Step add(String next) {
	    Step s = new Text(next);
	    this.next.add(s);
	    return s;
	}

	public String[] generate() {
	    Vector<String> result = new Vector<String>();
	    generate(new Stack<Step>(), result);
	    String[] s = new String[result.size()];
	    return result.toArray(s);
	}

    }

    private static class Text extends Step {

	private String txt;

	public Text(String txt) {
	    this.txt = txt;
	}

	@Override
	protected String out() {
	    return txt;
	}

    }

    private Step makeColumnContraint() {
	return null;
    }

    private Step makeColumnDef() {
	Step name = new Text("random_column_name");
	Step constraint = makeColumnContraint();

	name.add(" random_type_name").add(" " + constraint);
	name.add(" " + constraint);

	return name;
    }

    private Step makeCreateTableStmt() {
	Step create = new Text("CREATE");
	Step table = create.add(" TABLE");

	create.add(" TEMP").add(table);
	create.add(" TEMPORARY").add(table);
	table.add(" IF NOT EXISTS").add(" random_table_name").add(" (")
		.add(makeColumnDef()).add(")");

	return create;
    }

    @Override
    public void setUp() {
	d = new Database();
    }

    public void testCreateParsing() throws InvalidSqlQueryException {

	for (String s : makeCreateTableStmt().generate()) {
	    System.out.println(s);
	    try {
		d.prepare(s).run();
	    } catch (InvalidSqlQueryException e) {
		// TODO: Need to split the exception so that a special exception
		// is thrown when a computation failed, as opposed to the actual
		// parsing, e.g. not finding a table is not a parsing error.
	    }
	}
    }

}
