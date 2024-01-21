# Logical Plans & Expressions

_The source code discussed in this chapter can be found in the `logical-plan` module of the[ KQuery project](https://github.com/andygrove/how-query-engines-work)._

A logical plan represents a relation (a set of tuples) with a known schema. Each logical plan can have zero or more logical plans as inputs. It is convenient for a logical plan to expose its child plans so that a visitor pattern can be used to walk through the plan.

```kotlin
interface LogicalPlan {
  fun schema(): Schema
  fun children(): List<LogicalPlan>
}
```

## Printing Logical Plans

It is important to be able to print logical plans in human-readable form to help with debugging. Logical plans are typically printed as a hierarchical structure with child nodes indented.

We can implement a simple recursive helper function to format a logical plan.

```kotlin
fun format(plan: LogicalPlan, indent: Int = 0): String {
  val b = StringBuilder()
  0.rangeTo(indent).forEach { b.append("\t") }
  b.append(plan.toString()).append("\n")
  plan.children().forEach { b.append(format(it, indent+1)) }
  return b.toString()
}
```

Here is an example of a logical plan formatted using this method.

```
Projection: #id, #first_name, #last_name, #state, #salary
  Filter: #state = 'CO'
    Scan: employee.csv; projection=None
```

## Serialization

It is sometimes desirable to be able to serialize query plans so that they can easily be transferred to another process. It is good practice to add serialization early on as a precaution against accidentally referencing data structures that cannot be serialized (such as file handles or database connections).

One approach would be to use the implementation languages' default mechanism for serializing data structures to/from a format such as JSON. In Java, the Jackson library could be used. Kotlin has the `kotlinx.serialization` library, and Rust has a serde crate, for example.

Another option would be to define a language-agnostic serialization format using Avro, Thrift, or Protocol Buffers and then write code to translate between this format and the language-specific implementation.

Since publishing the first edition of this book, a new standard named ["substrait"](https://substrait.io/) has emerged, with the goal of providing cross-language serialization for relational algebra. I am excited about this project and predict that it will become the de-facto standard for representing query plans and open up many integration possibilities. For example, it would be possible to use a mature Java-based query planner such as Apache Calcite, serialize the plan in Substrait format, and then execute the plan in a query engine implemented in a lower-level language, such as C++ or Rust. For more information, visit https://substrait.io/.

## Logical Expressions

One of the fundamental building blocks of a query plan is the concept of an expression that can be evaluated against data at runtime.

Here are some examples of expressions that are typically supported in query engines.

| Expression            | Examples |
|-----------------------|----------|
| Literal Value         | "hello", 12.34 |
| Column Reference      | user_id, first_name, last_name |
| Math Expression       | salary * state_tax |
| Comparison Expression | x >= y |
| Boolean Expression    | birthday = today() AND age >= 21 |
| Aggregate Expression  | MIN(salary), MAX(salary), SUM(salary), AVG(salary), COUNT(*) |
| Scalar Function       | CONCAT(first_name, " ", last_name) |
| Aliased Expression    | salary * 0.02 AS pay_increase |

Of course, all of these expressions can be combined to form deeply nested expression trees. Expression evaluation is a textbook case of recursive programming.

When we are planning queries, we will need to know some basic metadata about the output of an expression. Specifically, we need to have a name for the expression so that other expressions can reference it and we need to know the data type of the values that the expression will produce when evaluated so that we can validate that the query plan is valid. For example, if we have an expression `a + b` then it can only be valid if both `a` and `b` are numeric types.

Also note that the data type of an expression can be dependent on the input data. For example, a column reference will have the data type of the column it is referencing, but a comparison expression always returns a Boolean value.

```kotlin
interface LogicalExpr {
  fun toField(input: LogicalPlan): Field
}
```

## Column Expressions

The `Column` expression simply represents a reference to a named column. The metadata for this expression is derived by finding the named column in the input and returning that column's metadata. Note that the term "column" here refers to a column produced by the input logical plan and could represent a column in a data source, or it could represent the result of an expression being evaluated against other inputs.

```kotlin
class Column(val name: String): LogicalExpr {

  override fun toField(input: LogicalPlan): Field {
    return input.schema().fields.find { it.name == name } ?:
      throw SQLException("No column named '$name'")
  }

  override fun toString(): String {
    return "#$name"
  }

}
```

## Literal Expressions

We need the ability to represent literal values as expressions so that we can write expressions such as `salary * 0.05`.

Here is an example expression for literal strings.

```kotlin
class LiteralString(val str: String): LogicalExpr {

  override fun toField(input: LogicalPlan): Field {
    return Field(str, ArrowTypes.StringType)
  }

  override fun toString(): String {
    return "'$str'"
  }

}
```

Here is an example expression for literal longs.

```kotlin
class LiteralLong(val n: Long): LogicalExpr {

  override fun toField(input: LogicalPlan): Field {
      return Field(n.toString(), ArrowTypes.Int64Type)
  }

  override fun toString(): String {
      return n.toString()
  }

}
```

## Binary Expressions

Binary expressions are simply expressions that take two inputs. There are three categories of binary expressions that we will implement, and those are comparison expressions, Boolean expressions, and math expressions. Because the string representation is the same for all of these, we can use a common base class that provides the `toString` method. The variables "l" and "r" refer to the left and right inputs.

```kotlin
abstract class BinaryExpr(
    val name: String,
    val op: String,
    val l: LogicalExpr,
    val r: LogicalExpr) : LogicalExpr {

  override fun toString(): String {
    return "$l $op $r"
  }
}
```

Comparison expressions such as `=` or `<` compare two values of the same data type and return a Boolean value. We also need to implement Boolean operators `AND` and `OR` which also take two arguments and produce a Boolean result, so we can use a common base class for these as well.

```kotlin
abstract class BooleanBinaryExpr(
    name: String,
    op: String,
    l: LogicalExpr,
    r: LogicalExpr) : BinaryExpr(name, op, l, r) {

  override fun toField(input: LogicalPlan): Field {
      return Field(name, ArrowTypes.BooleanType)
  }

}
```

This base class provides a concise way to implement the concrete comparison expressions.

## Comparison Expressions

```kotlin
/** Equality (`=`) comparison */
class Eq(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("eq", "=", l, r)

/** Inequality (`!=`) comparison */
class Neq(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("neq", "!=", l, r)

/** Greater than (`>`) comparison */
class Gt(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("gt", ">", l, r)

/** Greater than or equals (`>=`) comparison */
class GtEq(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("gteq", ">=", l, r)

/** Less than (`<`) comparison */
class Lt(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("lt", "<", l, r)

/** Less than or equals (`<=`) comparison */
class LtEq(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("lteq", "<=", l, r)
```

## Boolean Expressions

The base class also provides a concise way to implement the concrete Boolean logic expressions.

```kotlin
/** Logical AND */
class And(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("and", "AND", l, r)

/** Logical OR */
class Or(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("or", "OR", l, r)
```

## Math Expressions

Math expressions are another specialization of a binary expression. Math expressions typically operate on values of the same data type and produce a result of the same data type.

```kotlin
abstract class MathExpr(
    name: String,
    op: String,
    l: LogicalExpr,
    r: LogicalExpr) : BinaryExpr(name, op, l, r) {

  override fun toField(input: LogicalPlan): Field {
      return Field("mult", l.toField(input).dataType)
  }

}

class Add(l: LogicalExpr, r: LogicalExpr) : MathExpr("add", "+", l, r)
class Subtract(l: LogicalExpr, r: LogicalExpr) : MathExpr("subtract", "-", l, r)
class Multiply(l: LogicalExpr, r: LogicalExpr) : MathExpr("mult", "*", l, r)
class Divide(l: LogicalExpr, r: LogicalExpr) : MathExpr("div", "/", l, r)
class Modulus(l: LogicalExpr, r: LogicalExpr) : MathExpr("mod", "%", l, r)
```

## Aggregate Expressions

Aggregate expressions perform an aggregate function such as `MIN`, `MAX`, `COUNT`, `SUM`, or `AVG` on an input expression.

```kotlin
abstract class AggregateExpr(
    val name: String,
    val expr: LogicalExpr) : LogicalExpr {

  override fun toField(input: LogicalPlan): Field {
    return Field(name, expr.toField(input).dataType)
  }

  override fun toString(): String {
    return "$name($expr)"
  }
}
```

For aggregate expressions where the aggregated data type is the same as the input type, we can simply extend this base class.

```kotlin
class Sum(input: LogicalExpr) : AggregateExpr("SUM", input)
class Min(input: LogicalExpr) : AggregateExpr("MIN", input)
class Max(input: LogicalExpr) : AggregateExpr("MAX", input)
class Avg(input: LogicalExpr) : AggregateExpr("AVG", input)
```

For aggregate expressions where the data type is not dependent on the input type, we need to override the `toField` method. For example, the "COUNT" aggregate expression always produces an integer regardless of the data type of the values being counted.

```kotlin
class Count(input: LogicalExpr) : AggregateExpr("COUNT", input) {

  override fun toField(input: LogicalPlan): Field {
    return Field("COUNT", ArrowTypes.Int32Type)
  }

  override fun toString(): String {
    return "COUNT($expr)"
  }
}
```

## Logical Plans

With the logical expressions in place, we can now implement the logical plans for the various transformations that the query engine will support.

## Scan

The `Scan` logical plan represents fetching data from a `DataSource` with an optional projection. `Scan` is the only logical plan in our query engine that does not have another logical plan as an input. It is a leaf node in the query tree.

```kotlin
class Scan(
    val path: String,
    val dataSource: DataSource,
    val projection: List<String>): LogicalPlan {

  val schema = deriveSchema()

  override fun schema(): Schema {
    return schema
  }

  private fun deriveSchema() : Schema {
    val schema = dataSource.schema()
    if (projection.isEmpty()) {
      return schema
    } else {
      return schema.select(projection)
    }
  }

  override fun children(): List<LogicalPlan> {
    return listOf()
  }

  override fun toString(): String {
    return if (projection.isEmpty()) {
      "Scan: $path; projection=None"
    } else {
      "Scan: $path; projection=$projection"
    }
  }

}
```

## Projection

The `Projection` logical plan applies a projection to its input. A projection is a list of expressions to be evaluated against the input data. Sometimes this is as simple as a list of columns, such as `SELECT a, b, c FROM foo`, but it could also include any other type of expression that is supported. A more complex example would be `SELECT (CAST(a AS float) * 3.141592)) AS my_float FROM foo`.

```kotlin
class Projection(
    val input: LogicalPlan,
    val expr: List<LogicalExpr>): LogicalPlan {

  override fun schema(): Schema {
    return Schema(expr.map { it.toField(input) })
  }

  override fun children(): List<LogicalPlan> {
    return listOf(input)
  }

  override fun toString(): String {
    return "Projection: ${ expr.map {
        it.toString() }.joinToString(", ")
    }"
  }
}
```

## Selection (also known as Filter)

The `Selection` logical plan applies a filter expression to determine which rows should be selected (included) in its output. This is represented by the `WHERE` clause in SQL. A simple example would be `SELECT * FROM foo WHERE a > 5`. The filter expression needs to evaluate to a Boolean result.

```kotlin
class Selection(
    val input: LogicalPlan,
    val expr: Expr): LogicalPlan {

  override fun schema(): Schema {
    // selection does not change the schema of the input
    return input.schema()
  }

  override fun children(): List<LogicalPlan> {
    return listOf(input)
  }

  override fun toString(): String {
    return "Filter: $expr"
  }
}
```

### Aggregate

The `Aggregate` logical plan is more complex than `Projection`, `Selection`, or `Scan` and calculates aggregates of underlying data such as calculating minimum, maximum, averages, and sums of data. Aggregates are often grouped by other columns (or expressions). A simple example would be `SELECT job_title, AVG(salary) FROM employee GROUP BY job_title`.

```kotlin
class Aggregate(
    val input: LogicalPlan,
    val groupExpr: List<LogicalExpr>,
    val aggregateExpr: List<AggregateExpr>) : LogicalPlan {

  override fun schema(): Schema {
    return Schema(groupExpr.map { it.toField(input) } +
         		  aggregateExpr.map { it.toField(input) })
  }

  override fun children(): List<LogicalPlan> {
    return listOf(input)
  }

  override fun toString(): String {
    return "Aggregate: groupExpr=$groupExpr, aggregateExpr=$aggregateExpr"
  }
}
```

Note that in this implementation, the output of the aggregate plan is organized with grouping columns followed by aggregate expressions. It will often be necessary to wrap the aggregate logical plan in a projection so that columns are returned in the order requested in the original query.

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
