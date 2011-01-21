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

import java.util.ArrayList;
import java.util.Stack;
import java.util.Vector;

import junit.framework.TestCase;

import com.sonyericsson.prequel.Database;
import com.sonyericsson.prequel.InvalidSqlQueryException;

public class TestFreeForAll extends TestCase {

    private static final int RECURSION_DEPTH = 2;

    private Database d;

    private abstract static class Step {

	private int recursionCount;

	protected final Vector<Step> next;

	protected void generate(Stack<Step> path, Vector<String> result) {
	    if (path.contains(this)) {
		if (recursionCount == RECURSION_DEPTH - 1) {
		    return;
		}
		recursionCount++;
	    }
	    path.push(this);
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
	    if (path.contains(this)) {
		recursionCount--;
	    }
	}

	protected abstract String out();

	public Step() {
	    next = new Vector<Step>();
	}

	public void add(Step next) {
	    this.next.add(next);
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

	private final String txt;

	public Text(String txt) {
	    this.txt = txt;
	}

	@Override
	protected String out() {
	    return txt;
	}

	@Override
	public String toString() {
	    return txt;
	}

    }

    private static class Empty extends Step {

	@Override
	protected String out() {
	    return "";
	}

    }

    private static class Scope extends Empty {

	private final ArrayList<Step> bounds;

	private void defineBounds(Step current, Stack<Step> path) {
	    if (bounds.contains(current)) {
		return;
	    }
	    if (current.next.isEmpty()) {
		bounds.add(current);
	    } else {
		for (Step s : current.next) {
		    if (!path.contains(s)) {
			path.push(current);
			defineBounds(s, path);
			path.pop();
		    } else {
			bounds.add(current);
		    }
		}
	    }
	}

	public Scope(Step root) {
	    super.add(root);
	    bounds = new ArrayList<Step>();
	    defineBounds(root, new Stack<Step>());
	}

	@Override
	public void add(Step next) {
	    for (Step s : bounds) {
		s.add(next);
	    }
	}

    }

    private Step makeConflictClause() {
	Step root = new Empty();
	Step on = new Text(" ON");
	Step conflict = new Text(" CONFLICT");
	Step rollback = new Text(" ROLLBACK");
	Step abort = new Text(" ABORT");
	Step fail = new Text(" FAIL");
	Step ignore = new Text(" IGNORE");
	Step replace = new Text(" REPLACE");

	root.add(new Empty());
	root.add(on);
	on.add(conflict);
	conflict.add(rollback);
	conflict.add(abort);
	conflict.add(fail);
	conflict.add(ignore);
	conflict.add(replace);

	return root;
    }

    private Step makeColumnContraint() {
	Step root = new Empty();
	Step constraint = new Text(" CONSTRAINT");
	Step name = new Text(" constraint_name");
	Step primary = new Text(" PRIMARY");
	Step key = new Text(" KEY");
	Step asc = new Text(" ASC");
	Step desc = new Text(" DESC");
	Step conflict = makeConflictClause();
	Step autoincrement = new Text(" AUTOINCREMENT");

	root.add(primary);
	root.add(primary);
	root.add(new Empty());
	constraint.add(name);
	constraint.add(primary);
	name.add(primary);
	primary.add(key);
	key.add(asc);
	key.add(desc);
	key.add(conflict);
	asc.add(conflict);
	desc.add(conflict);
	conflict.add(autoincrement);

	return new Scope(root);
    }

    private Step makeTypeName() {
	Step name = new Text(" TEXT");

	name.add(name);
	name.add("(123)");
	name.add("(123, 456)");

	return new Scope(name);
    }

    private Step makeColumnDef() {
	Step name = new Text("column_name#"); // TODO: # replaced by level later
	Step type = makeTypeName();
	Step constraint = makeColumnContraint();

	name.add(type);
	name.add(constraint);
	type.add(constraint);

	return new Scope(name);
    }

    private Step makeCreateTableStmt() {
	Step create = new Text("CREATE");
	Step table = new Text(" TABLE");
	Step temp = new Text(" TEMP");
	Step temporary = new Text(" TEMPORARY");
	Step name = new Text(" table_name");
	Step exists = new Text(" IF NOT EXISTS");
	Step colDef = makeColumnDef();
	Step lpara = new Text(" (");
	Step rpara = new Text(")");
	Step comma = new Text(", ");

	lpara.add(colDef);
	colDef.add(rpara);
	colDef.add(comma);
	comma.add(colDef);
	exists.add(name);
	name.add(lpara);
	temp.add(table);
	temporary.add(table);
	table.add(name);
	table.add(exists);
	create.add(temp);
	create.add(temporary);
	create.add(table);

	return new Scope(create);
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
		System.out.println("****" + e.getMessage());
		// TODO: Need to split the exception so that a special exception
		// is thrown when a computation failed, as opposed to the actual
		// parsing, e.g. not finding a table is not a parsing error.
	    }
	}
    }

}
