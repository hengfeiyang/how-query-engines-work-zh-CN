# 构建逻辑计划

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `dataframe` 模块中找到。_

## 用困难的方式构建逻辑计划

既然我们已经为逻辑计划的子集定义了类，我们就可以以编程方式组合它们。

这里有一些具体的代码，用于构建针对包含列 `id, first_name, last_name, state, job_title, salary` 的 CSV 文件的查询 `SELECT * FROM employee WHERE state = 'CO'` 的计划。

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

此操作将打印以下计划：

```
Projection: #id, #first_name, #last_name, #state, #salary
    Filter: #state = 'CO'
        Scan: employee; projection=None
```

同样的代码也可以像这样写得更加简洁：

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

虽然这样更加简洁，但也更难以解释，所以最好能有一种更优雅的方式来创建逻辑计划。这就是 `DataFrame` 接口能够帮助到我们的地方。

## 使用 DataFrame 构建逻辑计划

实现一个 DataFrame 风格的 API 允许我们以一种更加用户友好的方式构建逻辑查询计划。`DataFrame` 只是逻辑查询计划的抽象，并且具有执行转换和操作的方法。它类似于 `fluent-style` 的构建器 API。

这是一个 `DataFrame` 接口的最小化示例，它允许我们将 映射（projection）和 过滤器（selection）应用于现有的 `DataFrame`。

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

下面是这个接口的实现。

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

在应用 映射（projection）或 过滤器（selection）之前，我们需要一种方法来创建表示底层数据源的初始 DataFrame。这通常是通过执行上下文获得的。

这是执行上下文的一个简单开始，我们稍后将对其进行增强。

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

有了这些基础工作，我们现在可以使用上下文和 DataFrame API 创建逻辑查询计划。

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

尽管如此清晰直观，但我们还能进一步添加一些方便的方法使其变得更易理解。这是 Kotlin 特有的，但其他语言也有类似的概念。

我们可以创建一些方便的方法来创建支持的表达式对象。

```kotlin
fun col(name: String) = Column(name)
fun lit(value: String) = LiteralString(value)
fun lit(value: Long) = LiteralLong(value)
fun lit(value: Double) = LiteralDouble(value)
```

我们还可以在 `LogicalExpr` 接口上定义中缀运算符来构建二元表达式。

```kotlin
infix fun LogicalExpr.eq(rhs: LogicalExpr): LogicalExpr { return Eq(this, rhs) }
infix fun LogicalExpr.neq(rhs: LogicalExpr): LogicalExpr { return Neq(this, rhs) }
infix fun LogicalExpr.gt(rhs: LogicalExpr): LogicalExpr { return Gt(this, rhs) }
infix fun LogicalExpr.gteq(rhs: LogicalExpr): LogicalExpr { return GtEq(this, rhs) }
infix fun LogicalExpr.lt(rhs: LogicalExpr): LogicalExpr { return Lt(this, rhs) }
infix fun LogicalExpr.lteq(rhs: LogicalExpr): LogicalExpr { return LtEq(this, rhs) }
```

有了这些方便的方法，我们现在可以编写富有表达力的代码来构建我们的逻辑查询计划了。

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

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
