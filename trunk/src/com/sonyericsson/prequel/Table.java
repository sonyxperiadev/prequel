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

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Vector;

public class Table {

    private static final String NULL_TEXT = "NULL";

    final static int NOT_NULL = 0x0100;

    final static int PRIMARY_KEY = 0x0200;

    final static int AUTO_INCREMENT = 0x0400;

    final static int ASCENDING = 0x0800;

    final static int DESCENDING = 0x1000;

    final static int TYPE_MASK = 0xff;

    final static int INTEGER = 0x00;

    final static int TEXT = 0x01;

    final static int REAL = 0x02;

    final static int NONE = 0x03;

    final static int NUMERIC = 0x04;

    private Vector<Row> rows;

    private Database parent;

    Vector<Object> defVals;

    Vector<Long> autoIncr;

    /* NOTE: Keep package private to allow quick access from inner classes. */
    Vector<String> columns;

    Vector<Integer> flags;

    private interface Row {

	Object set(int columnIdx, Object obj);

	Object get(int columnIdx);

	void addColumn(Object defVal);

	Row makeCopy();

    }

    @SuppressWarnings("serial")
    private static class VectorRow extends Vector<Object> implements Row {

	public VectorRow(int size) {
	    super(size);
	    setSize(size);
	}

	@Override
	public void addColumn(Object defVal) {
	    add(defVal);
	}

	@Override
	public Row makeCopy() {
	    return (Row) clone();
	}

    }

    class ObjectRow implements Row {

	Object o;

	public ObjectRow(Object obj) {
	    o = obj;
	}

	@Override
	public Object set(int columnIdx, Object obj) {
	    try {
		Class<?> clazz = o.getClass();
		String name = columns.get(columnIdx);
		Method m = clazz.getMethod(
			"set" + name.substring(0, 1).toUpperCase()
				+ name.substring(1), Object.class);
		m.invoke(o, obj);
	    } catch (SecurityException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (NoSuchMethodException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IllegalArgumentException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IllegalAccessException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (InvocationTargetException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	    return obj;
	}

	@Override
	public Object get(int columnIdx) {
	    try {
		Class<?> clazz = o.getClass();
		String name = columns.get(columnIdx);
		Method m = clazz.getMethod(
			"get" + name.substring(0, 1).toUpperCase()
				+ name.substring(1), Object.class);
		return m.invoke(o);
	    } catch (SecurityException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	    } catch (NoSuchMethodException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	    } catch (IllegalArgumentException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	    } catch (IllegalAccessException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	    } catch (InvocationTargetException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return null;
	    }
	}

	@Override
	public void addColumn(Object defVal) {
	    /*
	     * Attempt to set the default value. This fails if the new column is
	     * not present on the object.
	     */
	    set(columns.size() - 1, defVal);
	}

	@Override
	public Row makeCopy() {
	    return this;
	}

    }

    Table(Database parent) {
	this.parent = parent;
	columns = new Vector<String>();
	flags = new Vector<Integer>();
	defVals = new Vector<Object>();
	rows = new Vector<Row>();
	autoIncr = new Vector<Long>();
    }

    Table(Database parent, String name, int type, Object value) {
	this(parent);
	addColumn(name, type, null);
	set(0, 0, value);
    }

    Object convert(Object o, int type) {
	if (o == null) {
	    return null;
	}
	switch (type) {
	case INTEGER:
	    if (o instanceof Integer) {
		return o;
	    } else if (o instanceof Long) {
		long l = (Long) o;
		if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
		    return (int) l;
		} else {
		    return l;
		}
	    } else if (o instanceof Double) {
		return convert(((Double) o).intValue(), type);
	    } else if (o instanceof String) {
		return convert(Long.valueOf((String) o), type);
	    } else {
		return 0;
	    }
	case REAL:
	    if (o instanceof Integer) {
		return Double.valueOf((Integer) o);
	    } else if (o instanceof Long) {
		return Double.valueOf((Long) o);
	    } else if (o instanceof String) {
		return Double.valueOf((String) o);
	    } else {
		return 0d;
	    }
	case NUMERIC:
	case TEXT:
	case NONE:
	    return o.toString();
	default:
	    throw new IllegalArgumentException("Unknown type");
	}
    }

    void addColumn(String name, int flags, Object defVal) {
	columns.add(name);
	autoIncr.add(0l);
	this.flags.add(flags);
	defVals.add(convert(defVal, flags & TYPE_MASK));
	for (Row row : rows) {
	    row.addColumn(defVal);
	}
    }

    void addUniquness(int column) {
	// TODO
    }

    void set(int row, int column, Object value) {
	if (row >= rows.size()) {
	    rows.setSize(row + 1);
	}
	if (rows.get(row) == null) {
	    Row values = new VectorRow(columns.size());
	    for (int i = 0; i < flags.size(); i++) {
		if ((flags.get(i) & AUTO_INCREMENT) > 0) {
		    long incr = autoIncr.get(i);
		    autoIncr.set(i, incr + 1);
		    values.set(i, incr);
		} else if (defVals.get(i) != null) {
		    values.set(i, defVals.get(i));
		}
	    }
	    rows.set(row, values);
	}
	rows.get(row)
		.set(column, convert(value, flags.get(column) & TYPE_MASK));
    }

    Vector<Integer> getRowsWhere(Expression where) {
	Vector<Integer> result = new Vector<Integer>();
	for (int i = 0; i < rows.size(); i++) {
	    if (where == null || ((Integer) where.evaluate(this, i)) == 1) {
		result.add(i);
	    }
	}
	return result;
    }

    void copyRow(Table source, int idx) {
	int row = getRowCount();
	for (int i = 0; i < source.getColumnCount(); i++) {
	    int colIdx = getColumnIndex(source.getColumnName(i));
	    if (colIdx != -1) {
		set(row, colIdx, source.getCell(idx, i));
	    }
	}
    }

    int indexOf(Table other, int idx) {
	// TODO: Check in this table for a row that can be considered a equal to
	// the row at idx in the other table.
	return -1;
    }

    // Table ensureParent(Database parent) {
    // if (this.parent == parent) {
    // return this;
    // } else {
    // // TODO: Clone the table and return one that has the new parent!
    // return null;
    // }
    // }

    // void removeRow(int row) {
    // if (rows.size() > row) {
    // rows.remove(row);
    // }
    // }

    Table exract(Vector<Expression> columns, Expression where) {
	Table sub = new Table(parent);

	/* Create requested columns */
	if (columns == null) {
	    for (int i = 0; i < getColumnCount(); i++) {
		sub.addColumn(getColumnName(i), getFlags(i), getDefVal(i));
	    }
	    for (int row : getRowsWhere(where)) {
		sub.rows.add(rows.get(row).makeCopy());
	    }
	} else {
	    for (Expression column : columns) {
		String name = column.toString();
		name = name.substring(name.startsWith("(") ? 1 : 0,
			name.length() - (name.startsWith("(") ? 1 : 0));
		sub.addColumn(name, column.type(this), null);
	    }

	    /* Evaluate all cells */
	    int y = 0;
	    for (int row : getRowsWhere(where)) {
		int x = 0;
		for (Expression column : columns) {
		    sub.set(y, x, column.evaluate(this, row));
		    x++;
		}
		y++;
	    }
	}
	return sub;
    }

    void union(Table other, boolean allowDuplicates) {

	/* Add columns from other table */
	for (int i = 0; i < other.getColumnCount(); i++) {
	    addColumn(other.getColumnName(i), other.getFlags(i),
		    other.getDefVal(i));
	}

	/* Add rows from other table */
	for (int row : other.getRowsWhere(null)) {
	    if (allowDuplicates || indexOf(other, row) == -1) {
		copyRow(other, row);
	    }
	}
    }

    Object getDefVal(int idx) {
	return defVals.get(idx);
    }

    int getFlags(int idx) {
	return flags.get(idx);
    }

    Object getCell(int row, int column) {
	if (rows.size() > row) {
	    return rows.get(row).get(column);
	} else {
	    return null;
	}
    }

    /**
     * Gets the index of the column with the given name.
     * 
     * @param name
     *            The name of the column.
     * @return The index of the column, or -1 when there was no column with the
     *         given name.
     */
    public int getColumnIndex(String name) {
	return columns.indexOf(name);
    }

    /**
     * Gets the number of columns in the table.
     * 
     * @return The number of columns.
     */
    public int getColumnCount() {
	return columns.size();
    }

    /**
     * Gets the name of the specified column.
     * 
     * @param idx
     *            The column index.
     * @return The name of the column.
     * @throws IndexOutOfBoundsException
     *             when the index specifies a column that does not exist in the
     *             table.
     */
    public String getColumnName(int idx) throws IndexOutOfBoundsException {
	return columns.get(idx);
    }

    /**
     * Gets the number of rows in the table.
     * 
     * @return The number of table rows.
     */
    public int getRowCount() {
	return rows.size();
    }

    /**
     * Indicates whether the table is empty or not. An empty table is defined as
     * a table with no rows. The number of columns is ignored.
     * 
     * @return <code>true</code> when the table has no rows, <code>false</code>
     *         otherwise.
     */
    public boolean isEmpty() {
	return rows.isEmpty();
    }

    /**
     * Indicates whether a cell is set to the NULL value or not.
     * 
     * @param row
     *            The row of the cell.
     * @param column
     *            The column of the cell.
     * @return <code>true</code> when the cell is set to NULL,
     *         <code>false</code> otherwise.
     */
    public boolean isNull(int row, int column) {
	return (getCell(row, column) == null);
    }

    public int getCellInt(int row, int column) {
	Object o = getCell(row, column);
	if (o == null) {
	    return 0;
	} else if (o instanceof Integer) {
	    return (Integer) o;
	} else if (o instanceof Long) {
	    return ((Long) o).intValue();
	} else if (o instanceof Double) {
	    return ((Double) o).intValue();
	} else {
	    return (int) Long.parseLong(o.toString());
	}
    }

    public long getCellLong(int row, int column) {
	Object o = getCell(row, column);
	if (o == null) {
	    return 0;
	} else if (o instanceof Integer) {
	    return (Integer) o;
	} else if (o instanceof Long) {
	    return (Long) o;
	} else if (o instanceof Double) {
	    return ((Double) o).longValue();
	} else {
	    return Long.parseLong(o.toString());
	}
    }

    public double getCellDouble(int row, int column) {
	Object o = getCell(row, column);
	if (o == null) {
	    return 0;
	} else if (o instanceof Integer) {
	    return (Integer) o;
	} else if (o instanceof Double) {
	    return (Double) o;
	} else if (o instanceof Long) {
	    return (Long) o;
	} else {
	    return Double.parseDouble(o.toString());
	}
    }

    public String getCellString(int row, int column) {
	Object o = getCell(row, column);
	if (o == null) {
	    return "";
	} else {
	    return o.toString();
	}
    }

    public boolean getCellBoolean(int row, int column) {
	return (getCellInt(row, column) == 1);
    }

    @Override
    public String toString() {
	int[] maxWidth = new int[columns.size()];
	for (int i = 0; i < maxWidth.length; i++) {
	    maxWidth[i] = columns.get(i).length() + 2;
	    for (int j = 0; j < getRowCount(); j++) {
		Object entry = getCell(j, i);
		if (entry != null) {
		    maxWidth[i] = Math.max(maxWidth[i], entry.toString()
			    .length() + 2);
		} else {
		    maxWidth[i] = Math.max(maxWidth[i], NULL_TEXT.length() + 2);
		}
	    }
	}

	/* Generate headers */
	StringBuilder b = new StringBuilder();
	for (int i = 0; i < maxWidth.length; i++) {
	    b.append("+");
	    for (int j = 0; j < maxWidth[i]; j++) {
		b.append("-");
	    }
	}
	b.append("+\n");
	for (int i = 0; i < maxWidth.length; i++) {
	    b.append("| ");
	    b.append(columns.get(i));
	    for (int j = 0; j < maxWidth[i] - columns.get(i).length() - 1; j++) {
		b.append(" ");
	    }
	}
	b.append("|\n");
	for (int i = 0; i < maxWidth.length; i++) {
	    b.append("+");
	    for (int j = 0; j < maxWidth[i]; j++) {
		b.append("-");
	    }
	}
	b.append("+\n");

	/* Generate rows */
	for (int i = 0; i < getRowCount(); i++) {
	    for (int j = 0; j < maxWidth.length; j++) {
		b.append("| ");
		Object text = getCell(i, j);
		if (text == null) {
		    text = NULL_TEXT;
		}
		b.append(text);
		for (int k = 0; k < maxWidth[j] - text.toString().length() - 1; k++) {
		    b.append(" ");
		}
	    }
	    b.append("|\n");
	}

	/* Generate footer */
	for (int i = 0; i < maxWidth.length; i++) {
	    b.append("+");
	    for (int j = 0; j < maxWidth[i]; j++) {
		b.append("-");
	    }
	}
	b.append("+\n");
	return b.toString();
    }

    @SuppressWarnings("unchecked")
    public <T> T[] getRows(Class<T> clazz) {
	T[] temp = (T[]) Array.newInstance(clazz, rows.size());
	int len = 0;
	for (Row row : rows) {
	    if (row instanceof ObjectRow) {
		Object o = ((ObjectRow) row).o;
		if (clazz.isInstance(o)) {
		    temp[len++] = (T) o;
		}
	    }
	}
	final T[] result;
	if (temp.length == len) {
	    result = temp;
	} else {
	    result = (T[]) Array.newInstance(clazz, len);
	    System.arraycopy(temp, 0, result, 0, len);
	}
	return result;
    }

}
