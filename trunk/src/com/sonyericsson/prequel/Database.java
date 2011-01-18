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

import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

public class Database {

    private Table EMPTY_TABLE = new Table(this);

    private Object lock = new Object();

    private boolean dropped;

    private Database session;

    private Tokenizer tokenizer;

    private long userVersion;

    private Hashtable<String, Table> tables;

    private int lastBinding;

    private Hashtable<Integer, Object> bindings;

    @SuppressWarnings("serial")
    private static class ParsingException extends Exception {

	private int pos;

	public ParsingException(String reason, int pos) {
	    super(reason);
	    this.pos = pos;
	}

	public int getPos() {
	    return pos;
	}

    }

    private void ensureBegin() {
	// TODO: make sure we have a transaction
    }

    private void ensureEnd() {
	// TODO: make sure there are no un-ended transaction
    }

    private void beginTransaction() {
	// TODO: create work copy of the database
	session = new Database();
    }

    private void rollbackTransaction() {
	// TODO: same as "begin" for us actually
    }

    private void endTransaction() {
	// TODO: copy our working copy to the actual database
    }

    /**
     * Throws an exception indicating that an internal error has occurred.
     */
    private static void internalError() {
	throw new IllegalStateException("Internal error");
    }

    /**
     * Gets the specified table.
     * 
     * @param name
     *            The name of the table.
     * @return The table.
     * @throws ProcessingException
     *             when the given table did not exist.
     */
    private Table getTableSafe(String name) throws ProcessingException {
	Table result = tables.get(name);
	if (result == null) {
	    throw new ProcessingException("Table \"" + name
		    + "\" does not exist");
	}
	return result;
    }

    /**
     * Canonize an identifier name, i.e. identifiers that look like keywords and
     * therefore has got marked with apostrophes get these removed.
     * 
     * @param id
     *            The non-canonized identifier.
     * @return Canonized identifier.
     */
    private static String forceIdentifier(String id) {
	if (id.startsWith("'")) {
	    return id.substring(1, id.length() - 1);
	} else {
	    return id;
	}
    }

    /**
     * Checks if the identifiers are present for the next consumption. This
     * method does not affect the consumption state.
     * 
     * @param ids
     *            The identifiers to check for.
     * @return <code>true</code> when one of the identifiers are present,
     *         <code>false</code> otherwise.
     */
    private boolean lookAhead(String ids[]) {
	tokenizer.next();
	String command = tokenizer.current();
	tokenizer.currentAsNext();
	for (int i = 0; i < ids.length; i++) {
	    if ((ids[i] == null && command == null)
		    || (ids[i] != null && ids[i].equalsIgnoreCase(command))) {
		return true;
	    }
	}
	return false;
    }

    /**
     * Consumes one of the given identifiers, but also gives the option to
     * ignore the consumption in case none of the identifiers were there.
     * 
     * @param ids
     *            The identifiers.
     * @param acceptOther
     *            <code>true</code> to not throw an exception when none of the
     *            given identifier could not be consumed.
     * @return The index of the identifier that was consumed, or -1 in case
     *         nothing was consumed.
     * @throws ParsingException
     *             when no identifier could be consumed and
     *             <code>acceptOthers</code> was set to <code>false</code>.
     */
    private int eat(String ids[], boolean acceptOther) throws ParsingException {
	int startPos = tokenizer.getPos();
	tokenizer.next();
	String command = tokenizer.current();
	for (int i = 0; i < ids.length; i++) {
	    if ((ids[i] == null && command == null)
		    || (ids[i] != null && ids[i].equalsIgnoreCase(command))) {
		return i;
	    }
	}
	if (acceptOther) {
	    tokenizer.currentAsNext();
	    return -1;
	}
	StringBuilder alternatives = new StringBuilder();
	for (int i = 0; i < ids.length; i++) {
	    if (i > 0) {
		if (i == ids.length - 1) {
		    alternatives.append(" or ");
		} else {
		    alternatives.append(", ");
		}
	    }
	    alternatives.append(ids[i]);
	}
	throw new ParsingException(alternatives + " expected", startPos);
    }

    /**
     * Consumes the an identifier that <i>contains</i> any of the given strings.
     * 
     * @param ids
     *            The fragments to look for in the next token.
     * @return The index of the fragment that matches the identifier that was
     *         consumed, or -1 in case nothing was consumed.
     */
    private int eatFuzzy(String ids[]) {
	tokenizer.next();
	String command = tokenizer.current().toUpperCase();
	for (int i = 0; i < ids.length; i++) {
	    if ((ids[i] == null && command == null)
		    || (ids[i] != null && command
			    .contains(ids[i].toUpperCase()))) {
		return i;
	    }
	}
	tokenizer.currentAsNext();
	return -1;
    }

    /**
     * Consumes the given identifier, but gives the option to ignore the
     * consumption in case the identifier was not there.
     * 
     * @param id
     *            The identifier.
     * @param acceptOther
     *            <code>true</code> to not throw an exception when the given
     *            identifier could not be consumed.
     * @return <code>true</code> in case the identifier was consumed,
     *         <code>false</code> otherwise.
     * @throws ParsingException
     *             when the identifier could not be consumed and
     *             <code>acceptOthers</code> was set to <code>false</code>.
     */
    private boolean eat(String id, boolean acceptOther) throws ParsingException {
	return (eat(new String[] { id }, acceptOther) == 0);
    }

    /**
     * Consumes an integer.
     * 
     * @return The integer value.
     * @throws ParsingException
     *             when something else than an integer number was the next token
     *             to consume.
     */
    private int eatNumber() throws ParsingException {
	int startPos = tokenizer.getPos();
	tokenizer.next();
	if (tokenizer.currentIsNumber()) {
	    return Integer.parseInt(tokenizer.current());
	} else {
	    throw new ParsingException("Number expected", startPos);
	}
    }

    /**
     * Consumes a string (i.e. syntactically defined as a string).
     * 
     * @return The string value, without the quotes.
     * @throws ParsingException
     *             when something else than a string was the next token to
     *             consume.
     */
    private String eatString() throws ParsingException {
	int startPos = tokenizer.getPos();
	tokenizer.next();
	if (tokenizer.currentIsString()) {
	    String text = tokenizer.current();
	    return (text.substring(1, text.length() - 1));
	} else {
	    throw new ParsingException("String expected", startPos);
	}
    }

    /**
     * Consumes the given identifier.
     * 
     * @param id
     *            The identifier.
     * @throws ParsingException
     *             when something else than the given identifier number was the
     *             next token to consume.
     */
    private void eat(String id) throws ParsingException {
	eat(new String[] { id }, false);
    }

    /**
     * Consumes the next token as a literal.
     * 
     * @return The consumed token.
     * @throws ParsingException
     *             when there were nothing more to consume.
     */
    private String eat() throws ParsingException {
	int startPos = tokenizer.getPos();
	tokenizer.next();
	if (tokenizer.current() == null) {
	    throw new ParsingException("Identifier expected", startPos);
	}
	return tokenizer.current();
    }

    /**
     * Parses a logical SQL expression.
     * 
     * @return The expression.
     * @throws ParsingException
     *             when the expression contained syntactical errors.
     */
    private Expression parseExpression() throws ParsingException {
	Expression e = parseBinaryExpression(parseUnaryExpression(), 0);
	return e;
    }

    /**
     * Parses a unary logical SQL expression, e.g. NOT x.
     * 
     * @return The expression.
     * @throws ParsingException
     *             when the expression contained syntactical errors.
     */
    private Expression parseUnaryExpression() throws ParsingException {
	int startPos = tokenizer.getPos();
	eat();
	tokenizer.currentAsNext();
	if (eat("COALESCE", true)) {
	    // TODO: Make this a generic hook for functions!
	    Vector<Expression> params = new Vector<Expression>();
	    eat("(");
	    do {
		params.add(parseExpression());
	    } while (eat(",", true));
	    eat(")");
	    return new FunctionExpression("COALESCE", params);
	} else if (eat("NULL", true)) {
	    return new LiteralExpression(null);
	} else if (tokenizer.currentIsNumber()) {
	    String number = eat();
	    if (number.contains(".")) {
		return new LiteralExpression(Double.valueOf(number));
	    } else {
		long l = Long.valueOf(number);
		if (l > Integer.MAX_VALUE || l < Integer.MIN_VALUE) {
		    return new LiteralExpression(l);
		} else {
		    return new LiteralExpression((int) l);
		}
	    }
	} else if (tokenizer.currentIsString()) {
	    return new LiteralExpression(eatString());
	} else if (tokenizer.currentIsIdentifier()) {
	    return new ColumnExpression(eat());
	} else if (eat("NOT", true)) {
	    return new UnaryExpression(UnaryExpression.NOT,
		    parseUnaryExpression());
	} else if (eat("(", true)) {
	    Expression e = parseExpression();
	    eat(")");
	    return e;
	} else if (eat("?", true)) {
	    if (tokenizer.currentIsNumber()) {
		lastBinding = Integer.valueOf(eat());
	    }
	    /*
	     * This works since unbound parameters are treated as the NULL
	     * value.
	     */
	    Object value = bindings.get(lastBinding);
	    lastBinding++;
	    return new LiteralExpression(value);
	} else if (eat("-", true)) {
	    return new UnaryExpression(UnaryExpression.NEGATE,
		    parseExpression());
	} else {
	    throw new ParsingException("Expression expected", startPos);
	}
    }

    /**
     * Parses
     * <code>[ = | == | < | <= | > | >= | != | || | AND | OR | + | - | * | % | << | >> ]</code>
     * 
     * @return The index of the operator, or -1 when no match could be made.
     * @throws ParsingException
     *             when the expression contained syntactical errors.
     */
    private int parseOperator() {
	try {
	    if (eat("=", true) || eat("==", true)) {
		return BinaryExpression.EQUALS;
	    } else if (eat("<", true)) {
		return BinaryExpression.LESSER;
	    } else if (eat("<=", true)) {
		return BinaryExpression.LESSER_EQUALS;
	    } else if (eat(">", true)) {
		return BinaryExpression.GREATER;
	    } else if (eat(">=", true)) {
		return BinaryExpression.GREATER_EQUALS;
	    } else if (eat("!=", true)) {
		return BinaryExpression.NOT_EQUALS;
	    } else if (eat("||", true)) {
		return BinaryExpression.CONCAT;
	    } else if (eat("AND", true)) {
		return BinaryExpression.AND;
	    } else if (eat("OR", true)) {
		return BinaryExpression.OR;
	    } else if (eat("+", true)) {
		return BinaryExpression.ADD;
	    } else if (eat("-", true)) {
		return BinaryExpression.SUBTRACT;
	    } else if (eat("*", true)) {
		return BinaryExpression.MULTIPLY;
	    } else if (eat("/", true)) {
		return BinaryExpression.DIVIDE;
	    } else if (eat("%", true)) {
		return BinaryExpression.MODULUS;
	    } else if (eat("<<", true)) {
		return BinaryExpression.SHIFT_LEFT;
	    } else if (eat(">>", true)) {
		return BinaryExpression.SHIFT_RIGHT;
	    } else {
		return -1;
	    }
	} catch (ParsingException e) {
	    internalError();
	    return 0;
	}
    }

    /**
     * Parses the IN operator in a logical SQL expression.
     * 
     * @param left
     *            Expression to the left of the expression to be parsed.
     * @param not
     *            <code>true</code> represents the NOT IN combination,
     *            <code>false</code> the simple IN version.
     * @return The expression.
     * @throws ParsingException
     *             when the expression contained syntactical errors.
     */
    private Expression parseInExpression(Expression left, boolean not)
	    throws ParsingException {
	int num = 1;
	eat("(");
	StringBuilder selection = new StringBuilder();
	while (num > 0) {
	    String t = eat();
	    selection.append(t);
	    selection.append(" ");
	    if (t.equals("(")) {
		num++;
	    } else if (t.equals(")")) {
		num--;
	    }
	}
	return new InExpression(left, selection.toString(), not);
    }

    private Expression parseBinaryExpression(Expression left, int prevPrecedence)
	    throws ParsingException {
	while (true) {
	    int op = parseOperator();

	    /* Handle operators with special syntax */
	    if (op == -1) {
		if (eat("NOT", true)) {
		    switch (eat(new String[] { "NULL" }, false)) {
		    case 0:
			left = new UnaryExpression(UnaryExpression.NOT_NULL,
				left);
			break;
		    default:
			internalError();
		    }
		    break;
		} else if (eat("IN", true)) {
		    left = parseInExpression(left, false);
		    break;
		}
	    }

	    int precedence = BinaryExpression.getPrecedence(op);
	    if (op == -1 || precedence < prevPrecedence) {
		tokenizer.currentAsNext();
		break;
	    }
	    Expression right = parseUnaryExpression();
	    while (true) {
		int op2 = parseOperator();
		tokenizer.currentAsNext();
		int precedence2 = BinaryExpression.getPrecedence(op2);
		if (op2 == -1 || precedence2 <= precedence) {
		    break;
		} else {
		    right = parseBinaryExpression(right, precedence2);
		}
	    }
	    left = new BinaryExpression(left, op, right);
	}
	return left;
    }

    /**
     * Parses
     * <code>ON CONFLICT [ ROLLBACK | ABORT | FAIL | IGNORE | REPLACE ]</code>
     * 
     * @return Index of the action to be performed on conflict.
     * @throws ParsingException
     *             when a keyword was not encountered where expected.
     */
    private int parseConflictClause() throws ParsingException {
	if (eat("ON", true)) {
	    eat("CONFLICT");
	    return eat(new String[] { "ROLLBACK", "ABORT", "FAIL", "IGNORE",
		    "REPLACE" }, true);
	} else {
	    return -1;
	}
    }

    private void parseCreateTable() throws ParsingException,
	    ProcessingException {
	boolean exists = parseIfExists(true);
	String id = eat();
	eat("(");
	Table table = new Table(this);
	do {
	    if (eat("UNIQUE", true)) {
		eat("(");
		do {
		    table.addUniquness(table.getColumnIndex(eat()));
		} while (eat(",", true));
		eat(")");
		break;
	    }
	    int constraint = eat(new String[] { "PRIMARY", "UNIQUE" }, true);
	    if (constraint != -1) {
		switch (constraint) {
		case 0:
		    eat("KEY");
		    /* Fall through */
		case 1:
		    eat("(");
		    do {
			String column = eat();
		    } while (eat(",", true));
		    eat(")");
		    int action = parseConflictClause();
		    break;
		default:
		    internalError();
		}
		break;
	    } else {
		String name = eat();
		Object defVal = null;

		/*
		 * NOTE: Be forgiving when it comes to repeating the column name
		 * since it seems SQLite is as well.
		 */
		eat(name, true);

		/* Choose internal type */
		int type = -1;
		switch (eatFuzzy(new String[] { "INT", "CHAR", "CLOB", "TEXT",
			"BLOB", "REAL", "FLOA", "DOUB" })) {
		case 0:
		    type = Table.INTEGER;
		    break;
		case 1:
		case 2:
		case 3:
		    type = Table.TEXT;
		    if (eat("(", true)) {
			eatNumber();
			eat(")", true);
		    }
		    break;
		case 4:
		    type = Table.NONE;
		    break;
		case 5:
		case 6:
		case 7:
		    type = Table.REAL;
		    break;
		default:
		    break;
		}

		boolean exit = false;
		int flags = 0;
		while (!exit) {
		    int extra = eat(new String[] { "NOT", "PRIMARY", "UNIQUE",
			    "DEFAULT", "REFERENCES", "COLLATE" }, true);
		    if (type == -1 && extra == -1) {
			if (!lookAhead(new String[] { ",", ")" })) {
			    eat();
			    type = Table.NUMERIC;
			}
		    }
		    switch (extra) {
		    case 0:
			eat("NULL");
			flags |= Table.NOT_NULL;
			break;
		    case 1:
			eat("KEY");
			flags |= Table.PRIMARY_KEY;
			if (eat("AUTOINCREMENT", true)) {
			    flags |= Table.AUTO_INCREMENT;
			}
			break;
		    case 2:
			int resolution = parseConflictClause();
			break;
		    case 3:
			defVal = parseExpression().evaluate(null, -1);
			break;
		    case 4:
			String refTbl = eat();
			if (eat("(", true)) {
			    Vector<String> refCols = new Vector<String>();
			    do {
				refCols.add(eat());
			    } while (eat(",", true));
			    eat(")");
			}
			break;
		    case 5:
			int collation = parseCollation();
			break;
		    default:
			exit = true;
			break;
		    }
		}
		if (type == -1) {
		    type = Table.NONE;
		}
		table.addColumn(name, Math.max(0, type | flags), defVal);
	    }
	} while (eat(",", true));
	eat(")");

	/*
	 * On IF NOT EXISTS we skip adding the table when it is already present.
	 * This is done quietly according to specifications.
	 */
	if (tables.contains(id)) {
	    if (exists) {
		tables.put(id, table);
	    } else {
		throw new ProcessingException("Table \"" + id
			+ "\" already exists");
	    }
	} else {
	    tables.put(id, table);
	}
    }

    private boolean parseIfExists(boolean not) throws ParsingException {
	if (eat("IF", true)) {
	    if (not) {
		eat("NOT");
	    }
	    eat("EXISTS");
	    return true;
	} else {
	    return false;
	}
    }

    private void parseDropTable() throws ParsingException {
	boolean exists = parseIfExists(false);
	String id = eat();
	if (!exists && tables.get(id) == null) {
	    throw new IllegalArgumentException("Table \"" + id
		    + "\" does not exist");
	}
	tables.remove(id);
    }

    private void parseDropIndex() throws ParsingException {
	boolean exists = parseIfExists(false);
	String id = eat();
	// TODO: Remove the index!
    }

    private void parseDropView() throws ParsingException {
	boolean exists = parseIfExists(false);
	String id = eat();
	// TODO: Remove the view!
    }

    private void parseDropTrigger() throws ParsingException {
	boolean exists = parseIfExists(false);
	String id = eat();
	// TODO: Remove the trigger!
    }

    private void parseCreateTrigger() throws ParsingException {
	String id = eat();
	int timing = eat(new String[] { "BEFORE", "AFTER", "INSTEAD" }, true);
	if (timing == 2) {
	    eat("OF");
	}
	int type = eat(new String[] { "DELETE", "INSERT", "UPDATE" }, false);
	if (type == 2 && eat("OF", true)) {
	    do {
		String column = eat();
	    } while (eat(",", true));
	}
	eat("ON");
	String table = eat();
	if (eat("FOR", true)) {
	    eat("EACH");
	    eat("ROW");
	}
	if (eat("WHEN", true)) {
	    parseExpression();
	}
	eat("BEGIN");
	Vector<String> actions = new Vector<String>();
	do {
	    StringBuilder action = new StringBuilder();
	    while (!eat(";", true)) {
		action.append(eat());
		action.append(" ");
	    }
	    actions.add(action.toString());
	} while (!eat("END", true));
	// TODO: Store trigger!
    }

    private void parseCreateView() throws ParsingException {
	String name = eat();
	eat("AS");
	StringBuilder select = new StringBuilder();
	while (eat(new String[] { ";", null }, true) == -1) {
	    select.append(eat());
	    select.append(" ");
	}
	// TODO: Store the view!
    }

    private int parseCollation() throws ParsingException {
	return eat(new String[] { "BINARY", "NOCASE", "RTTRIM", "LOCALIZED",
		"UNICODE" }, false);
    }

    private void parseCreateIndex(boolean unique) throws ParsingException {
	boolean exists = parseIfExists(true);
	String indexName = eat();
	eat("ON");
	String tableName = eat();
	eat("(");
	do {
	    String column = eat();
	    if (eat("COLLATE", true)) {
		int collation = parseCollation();
	    }
	} while (eat(",", true));
	eat(")");
	// TODO: Store index!
    }

    private void parseCreate() throws ParsingException, ProcessingException {
	switch (eat(new String[] { "TABLE", "TRIGGER", "INDEX", "UNIQUE",
		"VIEW" }, false)) {
	case 0:
	    parseCreateTable();
	    break;
	case 1:
	    parseCreateTrigger();
	    break;
	case 2:
	    parseCreateIndex(false);
	    break;
	case 3:
	    eat("INDEX");
	    parseCreateIndex(true);
	    break;
	case 4:
	    parseCreateView();
	    break;
	default:
	    internalError();
	}
    }

    private void parseDrop() throws ParsingException {
	switch (eat(new String[] { "TABLE", "INDEX", "VIEW", "TRIGGER" }, false)) {
	case 0:
	    parseDropTable();
	    break;
	case 1:
	    parseDropIndex();
	    break;
	case 2:
	    parseDropView();
	    break;
	case 3:
	    parseDropTrigger();
	    break;
	default:
	    internalError();
	}
    }

    private Table parsePragma() throws ParsingException, ProcessingException {
	if (eat("user_version", true)) {
	    if (eat("=", true)) {
		userVersion = Long.valueOf(eat());
		return null;
	    } else {
		Table table = new Table(this);
		table.addColumn("user_version", 0, null);
		table.set(0, 0, userVersion);
		return table;
	    }
	} else if (eat("table_info", true)) {
	    eat("(");
	    String id = eat();
	    Table source = getTableSafe(id);
	    eat(")");
	    Table table = new Table(this);
	    table.addColumn("0", 0, null);
	    table.addColumn("name", 0, null);
	    table.addColumn("2", 0, null);
	    table.addColumn("3", 0, null);
	    table.addColumn("defVal", 0, null);
	    for (int i = 0; i < source.getColumnCount(); i++) {
		table.set(i, 1, source.getColumnName(i));
		table.set(i, 4, source.getDefVal(i));
	    }
	    return table;
	} else {
	    throw new IllegalArgumentException("Unsupported pragma");
	}
    }

    private void parseBegin() throws ParsingException {
	switch (eat(new String[] { "DEFERRED", "IMMEDIATE", "EXCLUSIVE" },
		false)) {
	case 2:
	    break;
	default:
	    internalError();
	}
	eat("TRANSACTION", true);
    }

    private void parseEnd() throws ParsingException {
	eat("TRANSACTION", true);
	endTransaction();
    }

    private void parseRollback() throws ParsingException {
	eat("TRANSACTION", true);
	rollbackTransaction();
    }

    private int parseInsert() throws ParsingException, ProcessingException {
	if (eat("OR", true)) {
	    int onInvalid = eat(new String[] { "IGNORE" }, false);
	}
	eat("INTO");
	String id = eat();
	eat("(");
	Vector<String> columns = new Vector<String>();
	do {
	    columns.add(forceIdentifier(eat()));
	} while (eat(",", true));
	eat(")");
	eat("VALUES");
	eat("(");
	Vector<Object> values = new Vector<Object>();
	Table table = getTableSafe(id);
	do {
	    if (values.size() == columns.size()) {
		throw new ProcessingException("More values than columns");
	    }
	    Object value = parseExpression().evaluate(table, -1);
	    int index = table.getColumnIndex(columns.get(values.size()));
	    if (index == -1) {
		throw new ProcessingException("Column \""
			+ columns.get(values.size())
			+ "\" not present in table \"" + id + "\"");
	    }
	    values.add(value);
	} while (eat(",", true));
	eat(")");
	if (columns.size() > values.size()) {
	    throw new ProcessingException("Fewer values than columns");
	}

	/*
	 * Put data into table. At this point we have already verified that all
	 * columns exists in this table, so no need to do it again.
	 */
	int row = table.getRowCount();
	for (int i = 0; i < columns.size(); i++) {
	    table.set(row, table.getColumnIndex(columns.get(i)), values.get(i));
	}
	return row;
    }

    private int parseDelete() throws ParsingException {
	eat("FROM");
	String id = eat();
	Expression exp = null;
	if (eat("WHERE", true)) {
	    exp = parseExpression();
	}
	// TODO: Support more stuff and actually delete the selected rows!
	return 0;
    }

    private void parseAttach() throws ParsingException {
	eat("DATABASE", true);
	String name = eat();
	eat("AS");
	String alias = eat();
    }

    private void parseAnalyze() throws ParsingException {
	if (eat(new String[] { ";", null }, true) == -1) {
	    String id = eat();
	}
	// TODO: Do something about this?
    }

    private Table parseDesc() throws ParsingException {
	String id = eat();
	Table tbl = tables.get(id);
	if (tbl == null) {
	    throw new IllegalArgumentException("Table \"" + id
		    + "\" does not exist");
	}
	Table t = new Table(this);
	t.addColumn("Field", Table.TEXT, null);
	t.addColumn("Type", Table.TEXT, null);
	t.addColumn("Null", Table.TEXT, null);
	t.addColumn("Key", Table.TEXT, "");
	t.addColumn("Default", Table.TEXT, "");
	t.addColumn("Extra", Table.TEXT, "");

	for (int i = 0; i < tbl.flags.size(); i++) {
	    t.set(i, 0, tbl.columns.get(i));
	    String type = "NONE";
	    int flags = tbl.flags.get(i);
	    switch (flags & Table.TYPE_MASK) {
	    case Table.INTEGER:
		type = "INTEGER";
		break;
	    case Table.NUMERIC:
		type = "NUMERIC";
		break;
	    case Table.TEXT:
		type = "TEXT";
		break;
	    case Table.REAL:
		type = "REAL";
		break;
	    default:
		break;
	    }
	    t.set(i, 1, type.toLowerCase());
	    t.set(i, 2, ((flags & Table.NOT_NULL) > 0) ? "NO" : "YES");
	    t.set(i, 3, ((flags & Table.PRIMARY_KEY) > 0) ? "PRI" : "");
	    Object def = tbl.defVals.get(i);
	    t.set(i, 4, (def != null) ? def : "");
	}

	return t;
    }

    private int parseUpdate() throws ParsingException, ProcessingException {
	if (eat("OR", true)) {
	    int onInvalid = eat(new String[] { "IGNORE" }, false);
	}
	String id = eat();
	eat("SET");
	Vector<String> columns = new Vector<String>();
	Vector<Object> values = new Vector<Object>();
	do {
	    columns.add(eat());
	    eat("=");
	    values.add(parseExpression());
	} while (eat(",", true));
	Expression exp = null;
	if (eat("WHERE", true)) {
	    exp = parseExpression();
	}
	Table table = getTableSafe(id);
	// Vector<Integer> rows = table.getRows(parseExpression());
	// for (int idx : rows) {
	// // TODO
	// }
	// return rows.size();
	return 0;
    }

    private Table parseSingleSource() throws ParsingException,
	    ProcessingException {
	Table result = null;
	if (eat("(", true)) {
	    if (eat("SELECT", true)) {
		result = parseSelect(null, -1);
	    } else {
		result = parseJoinSource();
	    }
	    eat(")");
	} else {
	    result = getTableSafe(eat());
	}

	/* Check if the table has been given an alias */
	if (eat("AS", true)) {
	    String alias = eat();
	}
	return result;
    }

    private Table parseJoinSource() throws ParsingException,
	    ProcessingException {
	Table result = parseSingleSource();
	int op = eat(new String[] { ",", "JOIN", "NATURAL", "LEFT", "OUTER",
		"INNER", "CROSS" }, true);
	if (op != -1) {
	    Table right = parseSingleSource();
	    switch (op) {
	    // TODO
	    default:
		internalError();
	    }
	}
	return result;
    }

    private Table parseSelect(Table left, int joiner) throws ParsingException,
	    ProcessingException {
	Table result = parseSelectCore();

	/*
	 * Due to the left to right evaluation of compounds we must perform the
	 * join with the previous table before we can parse a new one (otherwise
	 * this could have been handled nicely by the recursion).
	 */
	switch (joiner) {
	case 0:
	    result.union(left, false);
	    break;
	case 3:
	    result.union(left, true);
	    break;
	default:
	    if (left != null) {
		internalError();
	    }
	    break;
	}

	/* Check for compound */
	int op = eat(new String[] { "UNION", "INTERSECT", "EXCEPT" }, true);
	if (op == 0 && eat("ALL", true)) {
	    op = 3;
	}
	if (op != -1) {
	    eat("SELECT");
	    result = parseSelect(result, op);
	}

	/* Order the result */
	if (eat("ORDER", true)) {
	    eat("BY");
	    Vector<String> orderers = new Vector<String>();
	    do {
		orderers.add(eat());
	    } while (eat(",", true));
	    int ordering = eat(new String[] { "DESC", "ASC" }, true);
	}

	/* Limit result size */
	if (eat("LIMIT", true)) {
	    int first = eatNumber();
	    int type = eat(new String[] { ",", "OFFSET" }, true);
	    if (type != -1) {
		int second = eatNumber();
		switch (type) {
		case 0:
		    // TODO: Use these values to reduce size of subset!
		    break;
		case 1:
		    // TODO: Use these values to reduce size of subset!
		    break;
		default:
		    internalError();
		}
	    }
	}
	return result;
    }

    private Table parseSelectCore() throws ParsingException,
	    ProcessingException {
	// TODO: Add support for "AS" by parsing expressions instead of column
	// names, and then pass those expressions into the createSubset()! We
	// might also have to send the complete table list to that method, to be
	// able to support dot-notation for column references. And don't forget
	// that the expressions evaluate to column-aliases, so we need a
	// parallell array for that as well...

	/* Check if duplicates are allowed */
	boolean allowDuplicates = false;
	if (eat(new String[] { "ALL", "DISTINCT" }, true) < 1) {
	    allowDuplicates = true;
	}

	Vector<Expression> columns = null;
	if (!eat("*", true)) {
	    columns = new Vector<Expression>();
	    do {
		columns.add(parseExpression());
	    } while (eat(",", true));
	}
	eat("FROM");
	Table source = parseJoinSource();

	Expression exp = null;
	if (eat("WHERE", true)) {
	    exp = parseExpression();
	}
	if (eat("GROUP", true)) {
	    eat("BY");
	    Vector<String> group = new Vector<String>();
	    do {
		group.add(eat());
	    } while (eat(",", true));
	}
	return source.exract(columns, exp);
    }

    private Table parseSql() throws ParsingException, ProcessingException {
	Table result = EMPTY_TABLE;
	lastBinding = 0;
	switch (eat(new String[] { "CREATE", "DROP", "PRAGMA", "BEGIN", "END",
		"COMMIT", "ROLLBACK", "INSERT", "SELECT", "UPDATE", "DELETE",
		"ATTACH", "ANALYZE", "DESC" }, false)) {
	case 0:
	    ensureBegin();
	    parseCreate();
	    break;
	case 1:
	    ensureBegin();
	    parseDrop();
	    break;
	case 2:
	    ensureBegin();
	    result = parsePragma();
	    break;
	case 3:
	    parseBegin();
	    break;
	case 4:
	    /* Fall through */
	case 5:
	    parseEnd();
	    break;
	case 6:
	    parseRollback();
	    break;
	case 7:
	    result = new Table(this, "inserted_rows", Table.INTEGER,
		    parseInsert());
	    break;
	case 8:
	    result = parseSelect(null, -1);
	    break;
	case 9:
	    result = new Table(this, "updated_rows", Table.INTEGER,
		    parseUpdate());
	    break;
	case 10:
	    result = new Table(this, "deleted_rows", Table.INTEGER,
		    parseDelete());
	    break;
	case 11:
	    parseAttach();
	    break;
	case 12:
	    parseAnalyze();
	    break;
	case 13:
	    result = parseDesc();
	    break;
	default:
	    internalError();
	}
	eat(new String[] { ";", null }, false);
	return result;
    }

    void bind(int index, Object value) {
	if (value == null) {
	    bindings.remove(Integer.valueOf(index));
	} else {
	    bindings.put(Integer.valueOf(index), value);
	}
    }

    void clearBindings() {
	bindings.clear();
    }

    /**
     * <p>
     * Gets the database with the given file-name. This is used when the
     * <code>ATTACH DATABASE</code> statement is run to resolve which database
     * is referred to by the filename.
     * </p>
     * <p>
     * Override this method to be able to refer to other databases in your
     * SQL-queries.
     * <p>
     * 
     * @param fileName
     *            The file name given as first argument to
     *            <code>ATTACH DATABASE</code>.
     * @return The database corresponding to the given name, or
     *         <code>null</code> when no database corresponds to the given name.
     */
    // TODO: Still unclear if this should be a callback or a pre-link step like
    // the one for the tables!
    protected Database resolve(String fileName) {
	return null;
    }

    /**
     * Creates an empty database. The database will not have any links to any
     * other database.
     */
    public Database() {
	dropped = false;
	userVersion = 0;
	tables = new Hashtable<String, Table>();
	bindings = new Hashtable<Integer, Object>();

	/* Create default tables */
	try {
	    query("CREATE TABLE sqlite_stat1 (contacts TEXT, idx TEXT, tbl TEXT, stat TEXT)");
	} catch (InvalidSqlQueryException e) {
	    System.out.println(e);
	    internalError();
	}
    }

    // TODO public void link(String table, Collection rows)

    /**
     * Executes an SQL-query on the database.
     * 
     * TODO: Describe how to use object rows!
     * 
     * @param sql
     *            A valid query in SQLite-syntax.
     * @param params
     *            Zero or more objects that will be bound to parameters in the
     *            SQL-query. The order will be the same as given here.
     * @return The result of the query as a
     *         {@link com.sonyericsson.prequel.Table} . The contents of the
     *         table depends on the query e.g. INSERT, UPDATE and DELETE returns
     *         the number of affected rows, while SELECT returns the actual
     *         search result. Some queries produces no return values and the
     *         resulting table is therefore empty.
     */
    public Table query(String sql, Object... params)
	    throws InvalidSqlQueryException {
	synchronized (lock) {
	    if (dropped) {
		throw new IllegalStateException(
			"Cannot perform query on a dropped database");
	    } else {
		Table result = null;
		tokenizer = new Tokenizer(sql);
		for (int i = 0; i < params.length; i++) {
		    bind(i, params[i]);
		}
		try {
		    result = parseSql();
		    // TODO: A Query should be returned, which is then executed!
		} catch (ParsingException e) {
		    // e.printStackTrace();
		    throw new InvalidSqlQueryException(e.getMessage() + " at "
			    + e.getPos() + ": " + sql.substring(0, e.getPos())
			    + "<<here>>" + sql.substring(e.getPos()));
		} catch (ProcessingException e) {
		    throw new InvalidSqlQueryException(e.getMessage() + ": "
			    + sql);
		}
		ensureEnd();
		clearBindings();
		return result;
	    }
	}
    }

    @Override
    public String toString() {
	synchronized (lock) {
	    StringBuilder b = new StringBuilder();
	    b.append("Database@");
	    b.append(hashCode());
	    b.append("\n=====================\n");
	    b.append("user_version: ");
	    b.append(userVersion);
	    b.append("\n---------------------\n");
	    for (Map.Entry<String, Table> entry : tables.entrySet()) {
		if (!entry.getKey().equals("sqlite_stat1")) {
		    b.append(entry.getKey() + ":\n");
		    b.append(entry.getValue().toString());
		}
	    }
	    return b.toString();
	}
    }

}
