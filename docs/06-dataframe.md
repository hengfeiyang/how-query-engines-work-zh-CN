# Building Logical Plans

_The source code discussed in this chapter can be found in the `dataframe` module of the[ KQuery project](https://github.com/andygrove/how-query-engines-work)._

## Building Logical Plans The Hard Way

Now that we have defined classes for a subset of logical plans, we can combine them programmatically.

Here is some verbose code for building a plan for the query `SELECT * FROM employee WHERE state = 'CO'` against a CSV file containing the columns `id, first_name, last_name, state, job_title, salary`.

```kotlin
// create a plan to represent the data source
val csv = CsvDataSource("employee.csv")

// create a plan to represent the scan of the data source (FROM)
val scan = Scan("employee", csv, listOf())

// create a plan to represent the selection (WHERE)
val filterExpr = Eq(Column("state"), LiteralString("CO"))
val selection = Selection(scan, filterExpr)

// create a plan to represent the projection (SELECT)
val projectionList = listOf(Column("id"),
                            Column("first_name"),
                            Column("last_name"),
                            Column("state"),
                            Column("salary"))
val plan = Projection(selection, projectionList)

// print the plan
println(format(plan))
```

This prints the following plan:

```
Projection: #id, #first_name, #last_name, #state, #salary
    Filter: #state = 'CO'
        Scan: employee; projection=None
```

The same code can also be written more concisely like this:

```kotlin
val plan = Projection(
  Selection(
    Scan("employee", CsvDataSource("employee.csv"), listOf()),
    Eq(Column(3), LiteralString("CO"))
  ),
  listOf(Column("id"),
         Column("first_name"),
         Column("last_name"),
         Column("state"),
         Column("salary"))
)
println(format(plan))
```

Although this is more concise, it is also harder to interpret, so it would be nice to have a more elegant way to create logical plans. This is where a DataFrame interface can help.

## Building Logical Plans using DataFrames

Implementing a DataFrame style API allows us to build logical query plans in a much more user-friendly way. A DataFrame is just an abstraction around a logical query plan and has methods to perform transformations and actions. It is similar to a fluent-style builder API.

Here is a minimal starting point for a DataFrame interface that allows us to apply projections and selections to an existing DataFrame.

```kotlin
interface DataFrame {

  /** Apply a projection */
  fun project(expr: List<LogicalExpr>): DataFrame

  /** Apply a filter */
  fun filter(expr: LogicalExpr): DataFrame

  /** Aggregate */
  fun aggregate(groupBy: List<LogicalExpr>,
                aggregateExpr: List<AggregateExpr>): DataFrame

  /** Returns the schema of the data that will be produced by this DataFrame. */
  fun schema(): Schema

  /** Get the logical plan */
  fun logicalPlan() : LogicalPlan

}
```

Here is the implementation of this interface.

```kotlin
class DataFrameImpl(private val plan: LogicalPlan) : DataFrame {

  override fun project(expr: List<LogicalExpr>): DataFrame {
    return DataFrameImpl(Projection(plan, expr))
  }

  override fun filter(expr: LogicalExpr): DataFrame {
    return DataFrameImpl(Selection(plan, expr))
  }

  override fun aggregate(groupBy: List<LogicalExpr>,
                         aggregateExpr: List<AggregateExpr>): DataFrame {
    return DataFrameImpl(Aggregate(plan, groupBy, aggregateExpr))
  }

  override fun schema(): Schema {
    return plan.schema()
  }

  override fun logicalPlan(): LogicalPlan {
    return plan
  }

}
```

Before we can apply a projection or selection, we need a way to create an initial DataFrame that represents an underlying data source. This is usually obtained through an execution context.

Here is a simple starting point for an execution context that we will enhance later.

```kotlin
class ExecutionContext {

  fun csv(filename: String): DataFrame {
    return DataFrameImpl(Scan(filename, CsvDataSource(filename), listOf()))
  }

  fun parquet(filename: String): DataFrame {
    return DataFrameImpl(Scan(filename, ParquetDataSource(filename), listOf()))
  }
}
```

With this groundwork in place, we can now create a logical query plan using the context and the DataFrame API.

```kotlin
val ctx = ExecutionContext()

val plan = ctx.csv("employee.csv")
              .filter(Eq(Column("state"), LiteralString("CO")))
              .select(listOf(Column("id"),
                             Column("first_name"),
                             Column("last_name"),
                             Column("state"),
                             Column("salary")))
```

This is much cleaner and more intuitive, but we can go a step further and add some convenience methods to make this a little more comprehensible. This is specific to Kotlin, but other languages have similar concepts.

We can create some convenience methods for creating the supported expression objects.

```kotlin
fun col(name: String) = Column(name)
fun lit(value: String) = LiteralString(value)
fun lit(value: Long) = LiteralLong(value)
fun lit(value: Double) = LiteralDouble(value)
```

We can also define infix operators on the `LogicalExpr` interface for building binary expressions.

```kotlin
infix fun LogicalExpr.eq(rhs: LogicalExpr): LogicalExpr { return Eq(this, rhs) }
infix fun LogicalExpr.neq(rhs: LogicalExpr): LogicalExpr { return Neq(this, rhs) }
infix fun LogicalExpr.gt(rhs: LogicalExpr): LogicalExpr { return Gt(this, rhs) }
infix fun LogicalExpr.gteq(rhs: LogicalExpr): LogicalExpr { return GtEq(this, rhs) }
infix fun LogicalExpr.lt(rhs: LogicalExpr): LogicalExpr { return Lt(this, rhs) }
infix fun LogicalExpr.lteq(rhs: LogicalExpr): LogicalExpr { return LtEq(this, rhs) }
```

With these convenience methods in place, we can now write expressive code to build our logical query plan.

```kotlin
val df = ctx.csv(employeeCsv)
   .filter(col("state") eq lit("CO"))
   .select(listOf(
       col("id"),
       col("first_name"),
       col("last_name"),
       col("salary"),
       (col("salary") mult lit(0.1)) alias "bonus"))
   .filter(col("bonus") gt lit(1000))
```

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
