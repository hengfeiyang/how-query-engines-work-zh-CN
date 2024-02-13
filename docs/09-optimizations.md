# 查询优化

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `optimizer` 模块中找到。_

We now have functional query plans, but we rely on the end-user to construct the plans in an efficient way. For example, we expect the user to construct the plan so that filters happen as early as possible, especially before joins, since this limits the amount of data that needs to be processed.

This is a good time to implement a simple rules-based query optimizer that can re-arrange the query plan to make it more efficient.

This is going to become even more important once we start supporting SQL in chapter eleven, because the SQL language only defines how the query should work and does not always allow the user to specify the order that operators and expressions are evaluated in.

## Rule-Based Optimizations

Rule based optimizations are a simple and pragmatic approach to apply common sense optimizations to a query plan. These optimizations are typically executed against the logical plan before the physical plan is created, although rule-based optimizations can also be applied to physical plans.

The optimizations work by walking through the logical plan using the visitor pattern and creating a copy of each step in the plan with any necessary modifications applied. This is a much simpler design than attempting to mutate state while walking the plan and is well aligned with a functional programming style that prefers immutable state.

We will use the following interface to represent optimizer rules.

```kotlin
interface OptimizerRule {
  fun optimize(plan: LogicalPlan) : LogicalPlan
}
```

We will now look at some common optimization rules that most query engines implement.

### Projection Push-Down

The goal of the projection push-down rule is to filter out columns as soon as possible after reading data from disk and before other phases of query execution, to reduce the amount of data that is kept in memory (and potentially transfered over the network in the case of distributed queries) between operators.

In order to know which columns are referenced in a query, we must write recursive code to examine expressions and build up a list of columns.

```kotlin
fun extractColumns(expr: List<LogicalExpr>,
                   input: LogicalPlan,
                   accum: MutableSet<String>) {

  expr.forEach { extractColumns(it, input, accum) }
}

fun extractColumns(expr: LogicalExpr,
                   input: LogicalPlan,
                   accum: MutableSet<String>) {

  when (expr) {
    is ColumnIndex -> accum.add(input.schema().fields[expr.i].name)
    is Column -> accum.add(expr.name)
    is BinaryExpr -> {
       extractColumns(expr.l, input, accum)
       extractColumns(expr.r, input, accum)
    }
    is Alias -> extractColumns(expr.expr, input, accum)
    is CastExpr -> extractColumns(expr.expr, input, accum)
    is LiteralString -> {}
    is LiteralLong -> {}
    is LiteralDouble -> {}
    else -> throw IllegalStateException(
        "extractColumns does not support expression: $expr")
  }
}
```

With this utility code in place, we can go ahead and implement the optimizer rule. Note that for the `Projection`, `Selection`, and `Aggregate` plans we are building up the list of column names, but when we reach the `Scan` (which is a leaf node) we replace it with a version of the scan that has the list of column names used elsewhere in the query.

```kotlin
class ProjectionPushDownRule : OptimizerRule {

  override fun optimize(plan: LogicalPlan): LogicalPlan {
    return pushDown(plan, mutableSetOf())
  }

  private fun pushDown(plan: LogicalPlan,
                       columnNames: MutableSet<String>): LogicalPlan {
    return when (plan) {
      is Projection -> {
        extractColumns(plan.expr, columnNames)
        val input = pushDown(plan.input, columnNames)
        Projection(input, plan.expr)
      }
      is Selection -> {
        extractColumns(plan.expr, columnNames)
        val input = pushDown(plan.input, columnNames)
        Selection(input, plan.expr)
      }
      is Aggregate -> {
        extractColumns(plan.groupExpr, columnNames)
        extractColumns(plan.aggregateExpr.map { it.inputExpr() }, columnNames)
        val input = pushDown(plan.input, columnNames)
        Aggregate(input, plan.groupExpr, plan.aggregateExpr)
      }
      is Scan -> Scan(plan.name, plan.dataSource, columnNames.toList().sorted())
      else -> throw new UnsupportedOperationException()
    }
  }

}
```

Given this input logical plan:

```
Projection: #id, #first_name, #last_name
  Filter: #state = 'CO'
    Scan: employee; projection=None
```

This optimizer rule will transform it to the following plan.

```
Projection: #id, #first_name, #last_name
  Filter: #state = 'CO'
    Scan: employee; projection=[first_name, id, last_name, state]
```

### Predicate Push-Down

The Predicate Push-Down optimization aims to filter out rows as early as possible within a query, to avoid redundant processing. Consider the following which joins an `employee` table and `dept` table and then filters on employees based in Colorado.

```
Projection: #dept_name, #first_name, #last_name
  Filter: #state = 'CO'
    Join: #employee.dept_id = #dept.id
      Scan: employee; projection=[first_name, id, last_name, state]
      Scan: dept; projection=[id, dept_name]
```

The query will produce the correct results but will have the overhead of performing the join for all employees and not just those employees that are based in Colorado. The predicate push-down rule would push the filter down into the join as shown in the following query plan.

```
Projection: #dept_name, #first_name, #last_name
  Join: #employee.dept_id = #dept.id
    Filter: #state = 'CO'
      Scan: employee; projection=[first_name, id, last_name, state]
    Scan: dept; projection=[id, dept_name]
```

The join will now only process a subset of employees, resulting in better performance.

### Eliminate Common Subexpressions

Given a query such as `SELECT sum(price * qty) as total_price, sum(price * qty * tax_rate) as total_tax FROM ...`, we can see that the expression `price * qty` appears twice. Rather than perform this computation twice, we could choose to re-write the plan to compute it once.

Original plan:

```
Projection: sum(#price * #qty), sum(#price * #qty * #tax)
  Scan: sales
```

Optimized plan:

```
Projection: sum(#_price_mult_qty), sum(#_price_mult_qty * #tax)
  Projection: #price * #qty as _price_mult_qty
    Scan: sales
```

### Converting Correlated Subqueries to Joins

Given a query such as `SELECT id FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`, a simple implementation would be to scan all rows in `foo` and then perform a lookup in `bar` for each row in `foo`. This would be extremely inefficient, so query engines typically translate correlated subqueries into joins. This is also known as subquery decorrelation.

This query can be rewritten as `SELECT foo.id FROM foo JOIN bar ON foo.id = bar.id`.

```
Projection: foo.id
  LeftSemi Join: foo.id = bar.id
    TableScan: foo projection=[id]
    TableScan: bar projection=[id]
```

If the query is modified to use `NOT EXISTS` rather than `EXISTS` then the query plan would use a `LeftAnti` rather than `LeftSemi` join.

```
Projection: foo.id
  LeftAnti Join: foo.id = bar.id
    TableScan: foo projection=[id]
    TableScan: bar projection=[id]
```

## Cost-Based Optimizations

Cost-based optimization refers to optimization rules that use statistics about the underlying data to determine a cost of executing a particular query and then choose an optimal execution plan by looking for one with a low cost. Good examples would be choosing which join algorithm to use, or choosing which order tables should be joined in, based on the sizes of the underlying tables.

One major drawback to cost-based optimizations is that they depend on the availability of accurate and detailed statistics about the underlying data. Such statistics would typically include per-column statistics such as the number of null values, number of distinct values, min and max values, and histograms showing the distribution of values within the column. The histogram is essential to be able to detect that a predicate such as `state = 'CA'` is likely to produce more rows than `state = 'WY'` for example (California is the most populated US state, with 39 million residents, and Wyoming is the least populated state, with fewer than 1 million residents).

When working with file formats such as Orc or Parquet, some of these statistics are available, but generally it is necessary to run a process to build these statistics, and when working with multiple terabytes of data, this can be prohibitive, and outweigh the benefit, especially for ad-hoc queries.

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
