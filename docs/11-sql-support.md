# SQL Support

_The source code discussed in this chapter can be found in the `sql` module of the[ KQuery project](https://github.com/andygrove/how-query-engines-work)._

In addition to having the ability to hand-code logical plans, it would be more convenient in some cases to just write SQL. In this chapter, we will build a SQL parser and query planner that can translate SQL queries into logical plans.

## Tokenizer

The first step is to convert the SQL query string into a list of tokens representing keywords, literals, identifiers, and operators.

This is a subset of all possible tokens, but it is sufficient for now.

```kotlin
interface Token
data class IdentifierToken(val s: String) : Token
data class LiteralStringToken(val s: String) : Token
data class LiteralLongToken(val s: String) : Token
data class KeywordToken(val s: String) : Token
data class OperatorToken(val s: String) : Token
```

We will then need a tokenizer class. This is not particularly interesting to walk through here, and full source code can be found in the companion GitHub repository.

```kotlin
class Tokenizer {
  fun tokenize(sql: String): List<Token> {
    // see github repo for code
  }
}
```

Given the input `"SELECT a + b FROM c"` we expect the output to be as follows:

```kotlin
listOf(
  KeywordToken("SELECT"),
  IdentifierToken("a"),
  OperatorToken("+"),
  IdentifierToken("b"),
  KeywordToken("FROM"),
  IdentifierToken("c")
)
```

## Pratt Parser

We are going to hand-code a SQL parser based on the [Top Down Operator Precedence](https://tdop.github.io/) paper published by Vaughan R. Pratt in 1973. Although there are other approaches to building SQL parsers such as using Parser Generators and Parser Combinators, I have found Pratt's approach to work well and it results in code that is efficient, easy to comprehend, and easy to debug.

Here is a bare-bones implementation of a Pratt parser. In my opinion, it is beautiful in its simplicity. Expression parsing is performed by a simple loop that parses a "prefix" expression followed by an optional "infix" expression and keeps doing this until the precedence changes in such a way that the parser recognizes that it has finished parsing the expression. Of course, the implementation of `parsePrefix` and `parseInfix` can recursively call back into the `parse` method and this is where it becomes very powerful.

```kotlin
interface PrattParser {

  /** Parse an expression */
  fun parse(precedence: Int = 0): SqlExpr? {
    var expr = parsePrefix() ?: return null
    while (precedence < nextPrecedence()) {
      expr = parseInfix(expr, nextPrecedence())
    }
    return expr
  }

  /** Get the precedence of the next token */
  fun nextPrecedence(): Int

  /** Parse the next prefix expression */
  fun parsePrefix(): SqlExpr?

  /** Parse the next infix expression */
  fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr

}
```

This interface refers to a new `SqlExpr` class which will be our representation of a parsed expression and will largely be a one to one mapping to the expressions defined in the logical plan but for binary expressions we can use a more generic structure where the operator is a string rather than create separate data structures for all the different binary expressions that we will support.

Here are some examples of `SqlExpr` implementations.

```kotlin
/** SQL Expression */
interface SqlExpr

/** Simple SQL identifier such as a table or column name */
data class SqlIdentifier(val id: String) : SqlExpr {
  override fun toString() = id
}

/** Binary expression */
data class SqlBinaryExpr(val l: SqlExpr, val op: String, val r: SqlExpr) : SqlExpr {
  override fun toString(): String = "$l $op $r"
}

/** SQL literal string */
data class SqlString(val value: String) : SqlExpr {
  override fun toString() = "'$value'"
}
```

With these classes in place it is possible to represent the expression `foo = 'bar'` with the following code.

```kotlin
val sqlExpr = SqlBinaryExpr(SqlIdentifier("foo"), "=", SqlString("bar"))
```

## Parsing SQL Expressions

Let's walk through this approach for parsing a simple math expression such as `1 + 2 * 3`. This expression consists of the following tokens.

```kotlin
listOf(
  LiteralLongToken("1"),
  OperatorToken("+"),
  LiteralLongToken("2"),
  OperatorToken("*"),
  LiteralLongToken("3")
)
```

We need to create an implementation of the `PrattParser` trait and pass the tokens into the constructor. The tokens are wrapped in a `TokenStream` class that provides some convenience methods such as `next` for consuming the next token, and `peek` for when we want to look ahead without consuming a token.

```kotlin
class SqlParser(val tokens: TokenStream) : PrattParser {
}
```

Implementing the `nextPrecedence` method is simple because we only have a small number of tokens that have any precedence here and we need to have the multiplication and division operators have higher precedence than the addition and subtraction operator. Note that the specific numbers returned by this method are not important since they are just used for comparisons. A good reference for operator precedence can be found in the [PostgreSQL documentation](https://www.postgresql.org/docs/7.2/sql-precedence.html).

```kotlin
override fun nextPrecedence(): Int {
  val token = tokens.peek()
  return when (token) {
    is OperatorToken -> {
      when (token.s) {
        "+", "-" -> 50
        "*", "/" -> 60
        else -> 0
      }
    }
    else -> 0
  }
}
```

The prefix parser just needs to know how to parse literal numeric values.

```kotlin
override fun parsePrefix(): SqlExpr? {
  val token = tokens.next() ?: return null
  return when (token) {
    is LiteralLongToken -> SqlLong(token.s.toLong())
    else -> throw IllegalStateException("Unexpected token $token")
  }
}
```

The infix parser just needs to know how to parse operators. Note that after parsing an operator, this method recursively calls back into the top level `parse` method to parse the expression following the operator (the right-hand side of the binary expression).

```kotlin
override fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr {
  val token = tokens.peek()
  return when (token) {
    is OperatorToken -> {
      tokens.next()
      SqlBinaryExpr(left, token.s, parse(precedence) ?:
                    throw SQLException("Error parsing infix"))
    }
    else -> throw IllegalStateException("Unexpected infix token $token")
  }
}
```

The precedence logic can be demonstrated by parsing the math expressions `1 + 2 * 3` and `1 * 2 + 3` which should be parsed as `1 + (2 * 3)` and` (1 * 2) + 3` respectively.

*Example: Parsing `1 + 2 _ 3`*

These are the tokens along with their precedence values.

```
Tokens:      [1]  [+]  [2]  [*]  [3]
Precedence:  [0] [50]  [0] [60]  [0]
```

The final result correctly represents the expression as `1 + (2 * 3)`.

```kotlin
SqlBinaryExpr(
    SqlLong(1),
    "+",
    SqlBinaryExpr(SqlLong(2), "*", SqlLong(3))
)
```

*Example: Parsing `1 _ 2 + 3`*

```
Tokens:      [1]  [*]  [2]  [+]  [3]
Precedence:  [0] [60]  [0] [50]  [0]
```

The final result correctly represents the expression as `(1 * 2) + 3`.

```kotlin
SqlBinaryExpr(
    SqlBinaryExpr(SqlLong(1), "*", SqlLong(2)),
    "+",
    SqlLong(3)
)
```

## Parsing a SELECT statement

Now that we have the ability to parse some simple expressions, the next step is to extend the parser to support parsing a SELECT statement into a concrete syntax tree (CST). Note that with other approaches to parsing such as using a parser generator like ANTLR there is an intermediate stage known as an Abstract Syntax Tree (AST) which then needs to be translated to a Concrete Syntax Tree but with the Pratt Parser approach we go directly from tokens to the CST.

Here is an example CST that can represent a simple single-table query with a projection and selection. This will be extended to support more complex queries in later chapters.

```kotlin
data class SqlSelect(
    val projection: List<SqlExpr>,
    val selection: SqlExpr,
    val tableName: String) : SqlRelation
```

## SQL Query Planner

The SQL Query Planner translates the SQL Query Tree into a Logical Plan. This turns out to be a much harder problem than translating a logical plan to a physical plan due to the flexibility of the SQL language. For example, consider the following simple query.

```sql
SELECT id, first_name, last_name, salary/12 AS monthly_salary
FROM employee
WHERE state = 'CO' AND monthly_salary > 1000
```

Although this is intuitive to a human reading the query, the selection part of the query (the `WHERE` clause) refers to one expression (`state`) that is not included in the output of the projection so clearly needs to be applied before the projection but also refers to another expression (`salary/12 AS monthly_salary`) which is only available after the projection is applied. We will face similar issues with the `GROUP BY`, `HAVING`, and `ORDER BY` clauses.

There are multiple solutions to this problem. One approach would be to translate this query to the following logical plan, splitting the selection expression into two steps, one before and one after the projection. However, this is only possible because the selection expression is a conjunctive predicate (the expression is true only if all parts are true) and this approach might not be possible for more complex expressions. If the expression had been `state = 'CO' OR monthly_salary > 1000` then we could not do this.

```
Filter: #monthly_salary > 1000
  Projection: #id, #first_name, #last_name, #salary/12 AS monthly_salary
    Filter: #state = 'CO'
      Scan: table=employee
```

A simpler and more generic approach would be to add all the required expressions to the projection so that the selection can be applied after the projection, and then remove any columns that were added by wrapping the output in another projection.

```
Projection: #id, #first_name, #last_name, #monthly_salary
  Filter: #state = 'CO' AND #monthly_salary > 1000
    Projection: #id, #first_name, #last_name, #salary/12 AS monthly_salary, #state
      Scan: table=employee
```

It is worth noting that we will build a "Predicate Push Down" query optimizer rule in a later chapter that will be able to optimize this plan and push the `state = 'CO'` part of the predicate further down in the plan so that it is before the projection.

## Translating SQL Expressions

Translating SQL expressions to logical expressions is fairly simple, as demonstrated in this example code.

```kotlin
private fun createLogicalExpr(expr: SqlExpr, input: DataFrame) : LogicalExpr {
  return when (expr) {
    is SqlIdentifier -> Column(expr.id)
    is SqlAlias -> Alias(createLogicalExpr(expr.expr, input), expr.alias.id)
    is SqlString -> LiteralString(expr.value)
    is SqlLong -> LiteralLong(expr.value)
    is SqlDouble -> LiteralDouble(expr.value)
    is SqlBinaryExpr -> {
      val l = createLogicalExpr(expr.l, input)
      val r = createLogicalExpr(expr.r, input)
      when(expr.op) {
        // comparison operators
        "=" -> Eq(l, r)
        "!=" -> Neq(l, r)
        ">" -> Gt(l, r)
        ">=" -> GtEq(l, r)
        "<" -> Lt(l, r)
        "<=" -> LtEq(l, r)
        // boolean operators
        "AND" -> And(l, r)
        "OR" -> Or(l, r)
        // math operators
        "+" -> Add(l, r)
        "-" -> Subtract(l, r)
        "*" -> Multiply(l, r)
        "/" -> Divide(l, r)
        "%" -> Modulus(l, r)
        else -> throw SQLException("Invalid operator ${expr.op}")
      }
    }

    else -> throw new UnsupportedOperationException()
  }
}
```

## Planning SELECT

If we only wanted to support the use case where all columns referenced in the selection also exist in the projection we could get away with some very simple logic to build the query plan.

```kotlin
fun createDataFrame(select: SqlSelect, tables: Map<String, DataFrame>) : DataFrame {

  // get a reference to the data source
  var df = tables[select.tableName] ?:
      throw SQLException("No table named '${select.tableName}'")

  val projectionExpr = select.projection.map { createLogicalExpr(it, df) }

  if (select.selection == null) {
    // apply projection
    return df.select(projectionExpr)
  }

  // apply projection then wrap in a selection (filter)
  return df.select(projectionExpr)
           .filter(createLogicalExpr(select.selection, df))
}
```

However, because the selection could reference both inputs to the projections and outputs from the projection we need to create a more complex plan with an intermediate projection. The first step is to determine which columns are references by the selection filter expression. To do this we will use the visitor pattern to walk the expression tree and build a mutable set of column names.

Here is the utility method we will use to walk the expression tree.

```kotlin
private fun visit(expr: LogicalExpr, accumulator: MutableSet<String>) {
  when (expr) {
    is Column -> accumulator.add(expr.name)
    is Alias -> visit(expr.expr, accumulator)
    is BinaryExpr -> {
      visit(expr.l, accumulator)
      visit(expr.r, accumulator)
     }
  }
}
```

With this in place we can now write the following code to convert a SELECT statement into a valid logical plan. This code sample is not perfect and probably contains some bugs for edge cases where there are name clashes between columns in the data source and aliased expressions but we will ignore this for the moment to keep the code simple.

```kotlin
fun createDataFrame(select: SqlSelect, tables: Map<String, DataFrame>) : DataFrame {

  // get a reference to the data source
  var df = tables[select.tableName] ?:
    throw SQLException("No table named '${select.tableName}'")

  // create the logical expressions for the projection
  val projectionExpr = select.projection.map { createLogicalExpr(it, df) }

  if (select.selection == null) {
    // if there is no selection then we can just return the projection
    return df.select(projectionExpr)
  }

  // create the logical expression to represent the selection
  val filterExpr = createLogicalExpr(select.selection, df)

  // get a list of columns references in the projection expression
  val columnsInProjection = projectionExpr
    .map { it.toField(df.logicalPlan()).name}
    .toSet()

  // get a list of columns referenced in the selection expression
  val columnNames = mutableSetOf<String>()
  visit(filterExpr, columnNames)

  // determine if the selection references any columns not in the projection
  val missing = columnNames - columnsInProjection

  // if the selection only references outputs from the projection we can
  // simply apply the filter expression to the DataFrame representing
  // the projection
  if (missing.size == 0) {
    return df.select(projectionExpr)
             .filter(filterExpr)
  }

  // because the selection references some columns that are not in the
  // projection output we need to create an interim projection that has
  // the additional columns and then we need to remove them after the
  // selection has been applied
  return df.select(projectionExpr + missing.map { Column(it) })
           .filter(filterExpr)
           .select(projectionExpr.map {
              Column(it.toField(df.logicalPlan()).name)
            })
}
```

## Planning for Aggregate Queries

As you can see, the SQL query planner is relatively complex and the code for parsing aggregate queries is quite involved. If you are interested in learning more, please refer to the source code.

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
