Prequel
=======
An in-memory Java-only database supporting SQLite syntax.

Introduction
------------

Provides a class that acts as a complete SQL-database which can be written and read from using SQLite syntax. Interaction with the database is made through one single query method, making it very easy to use.

Purposes
--------

The primary purpose of this library is to make it easy to try out SQL in a Java context. The source is not optimized for speed and does is in its current state not offer complete SQLite syntax support.

Secondary purpose of this library is to make SQL available in Java at very low introduction cost. If you know SQL you can start use this library to keep in-memory application databases and make database lookup instantly. No need to set up any sub-systems.

Quick Start
-----------

This:
```java
Database d = new Database();
d.query("CREATE TABLE bank (name TEXT, money INTEGER)");
d.query("INSERT INTO bank VALUES ('Donald Duck', 100)");
d.query("INSERT INTO bank VALUES ('Scrooge McDuck', 1000000)");
System.out.println(d.query("SELECT * FROM bank WHERE money > 500"));
```

...outputs:
<pre>
 +----------------+---------+
 | name           | credits |
 +----------------+---------+
 | Scrooge McDuck | 1000000 |
 +----------------+---------+
</pre>

Disclaimer
----------

The source code is in its current state a result of trying to learn a little more about SQL and at the same time produce a simple SQL-database environment to play around with. As such it is a little bit messy and suffers some from ad-hoc design (which might be a good thing or a bad thing depending on what development paradigm you dig). 
