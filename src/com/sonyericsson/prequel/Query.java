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

public class Query {

    private Database origin;

    private String sql;

    Query(Database origin, String sql) {
	this.origin = origin;
	this.sql = sql;
    }

    /**
     * Runs a previously compiled SQL-query using the given parameters.
     * 
     * @param params
     *            Zero or more objects that will be bound to parameters in the
     *            SQL-query. The order will be the same as given here.
     * 
     * @return The result of the query as a
     *         {@link com.sonyericsson.prequel.Table} . The contents of the
     *         table depends on the query e.g. INSERT, UPDATE and DELETE returns
     *         the number of affected rows, while SELECT returns the actual
     *         search result. Some queries produces no return values and the
     *         resulting table is therefore empty.
     * @throws InvalidSqlQueryException
     */
    public Table run(Object... params) throws InvalidSqlQueryException {
	return origin.exec(sql, params);
    }

    @Override
    public String toString() {
	return sql;
    }

}
