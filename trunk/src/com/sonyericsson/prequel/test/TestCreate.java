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

import junit.framework.TestCase;

import com.sonyericsson.prequel.Database;
import com.sonyericsson.prequel.InvalidSqlQueryException;
import com.sonyericsson.prequel.Query;
import com.sonyericsson.prequel.Table;

public class TestCreate extends TestCase {

    private Database d;

    @Override
    public void setUp() {
	d = new Database();
    }

    public void testReturnValue() throws InvalidSqlQueryException {
	Table t = d.prepare("CREATE TABLE test_table (a);").run();
	assertNotNull(t);
	assertTrue(t.isEmpty());
    }

    public void testSeveralColumns() throws InvalidSqlQueryException {
	Table t = d
		.prepare(
			"CREATE TABLE test_table (ax123, b, cx123456789, d, ex1, fx123456789, gx1, h);")
		.run();
	assertNotNull(t);
	assertTrue(t.isEmpty());
    }

    public void testDifferentColumnTypes() throws InvalidSqlQueryException {
	Table t = d
		.prepare(
			"CREATE TABLE test_table (ax123 INTEGER, b, cx123456789 VARCHAR(123), d BLOB, ex1 NUMERIC, fx123456789 REAL, gx1 FLOAT, h TEXT);")
		.run();
	assertNotNull(t);
	assertTrue(t.isEmpty());
    }

}
