# 查询规划器

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `query-planner` 模块中找到。_

We have defined logical and physical query plans, and now we need a query planner that can translate the logical plan into the physical plan.

The query planner may choose different physical plans based on configuration options or based on the target platform's hardware capabilities. For example, queries could be executed on CPU or GPU, on a single node, or distributed in a cluster.

## Translating Logical Expressions

The first step is to define a method to translate logical expressions to physical expressions recursively. The following code sample demonstrates an implementation based on a switch statement and shows how translating a binary expression, which has two input expressions, causes the code to recurse back into the same method to translate those inputs. This approach walks the entire logical expression tree and creates a corresponding physical expression tree.

```kotlin
fun createPhysicalExpr(expr: LogicalExpr,
                       input: LogicalPlan): PhysicalExpr = when (expr) {
  is ColumnIndex -> ColumnExpression(expr.i)
  is LiteralString -> LiteralStringExpression(expr.str)
  is BinaryExpr -> {
    val l = createPhysicalExpr(expr.l, input)
    val r = createPhysicalExpr(expr.r, input)
    ...
  }
  ...
}
```

The following sections will explain the implementation for each type of expression.

## Column Expressions

The logical Column expression references columns by name, but the physical expression uses column indices for improved performance, so the query planner needs to perform the translation from column name to column index and throw an exception if the column name is not valid.

This simplified example looks for the first matching column name and does not check if there are multiple matching columns, which should be an error condition.

```kotlin
is Column -> {
  val i = input.schema().fields.indexOfFirst { it.name == expr.name }
  if (i == -1) {
    throw SQLException("No column named '${expr.name}'")
  }
  ColumnExpression(i)
```

## Literal Expressions

The physical expressions for literal values are straightforward, and the mapping from logical to physical expression is trivial because we need to copy the literal value over.

```kotlin
is LiteralLong -> LiteralLongExpression(expr.n)
is LiteralDouble -> LiteralDoubleExpression(expr.n)
is LiteralString -> LiteralStringExpression(expr.str)
```

## Binary Expressions

To create a physical expression for a binary expression we first need to create the physical expression for the left and right inputs and then we need to create the specific physical expression.

```kotlin
is BinaryExpr -> {
  val l = createPhysicalExpr(expr.l, input)
  val r = createPhysicalExpr(expr.r, input)
  when (expr) {
    // comparision
    is Eq -> EqExpression(l, r)
    is Neq -> NeqExpression(l, r)
    is Gt -> GtExpression(l, r)
    is GtEq -> GtEqExpression(l, r)
    is Lt -> LtExpression(l, r)
    is LtEq -> LtEqExpression(l, r)

    // boolean
    is And -> AndExpression(l, r)
    is Or -> OrExpression(l, r)

    // math
    is Add -> AddExpression(l, r)
    is Subtract -> SubtractExpression(l, r)
    is Multiply -> MultiplyExpression(l, r)
    is Divide -> DivideExpression(l, r)

    else -> throw IllegalStateException(
        "Unsupported binary expression: $expr")
    }
}
```

## Translating Logical Plans

We need to implement a recursive function to walk the logical plan tree and translate it into a physical plan, using the same pattern described earlier for translating expressions.

```kotlin
fun createPhysicalPlan(plan: LogicalPlan) : PhysicalPlan {
  return when (plan) {
    is Scan -> ...
    is Selection -> ...
    ...
}
```

## Scan

Translating the Scan plan simply requires copying the data source reference and the logical plan's projection.

```kotlin
is Scan -> ScanExec(plan.dataSource, plan.projection)
```

## Projection

There are two steps to translating a projection. First, we need to create a physical plan for the projection's input, and then we need to convert the projection's logical expressions to physical expressions.

```kotlin
is Projection -> {
  val input = createPhysicalPlan(plan.input)
  val projectionExpr = plan.expr.map { createPhysicalExpr(it, plan.input) }
  val projectionSchema = Schema(plan.expr.map { it.toField(plan.input) })
  ProjectionExec(input, projectionSchema, projectionExpr)
}
```

## Selection (also known as Filter)

The query planning step for `Selection` is very similar to `Projection`.

```kotlin
is Selection -> {
  val input = createPhysicalPlan(plan.input)
  val filterExpr = createPhysicalExpr(plan.expr, plan.input)
  SelectionExec(input, filterExpr)
}
```

## Aggregate

The query planning step for aggregate queries involves evaluating the expressions that define the optional grouping keys and evaluating the expressions that are the inputs to the aggregate functions, and then creating the physical aggregate expressions.

```kotlin
is Aggregate -> {
  val input = createPhysicalPlan(plan.input)
  val groupExpr = plan.groupExpr.map { createPhysicalExpr(it, plan.input) }
  val aggregateExpr = plan.aggregateExpr.map {
    when (it) {
      is Max -> MaxExpression(createPhysicalExpr(it.expr, plan.input))
      is Min -> MinExpression(createPhysicalExpr(it.expr, plan.input))
      is Sum -> SumExpression(createPhysicalExpr(it.expr, plan.input))
      else -> throw java.lang.IllegalStateException(
          "Unsupported aggregate function: $it")
    }
  }
  HashAggregateExec(input, groupExpr, aggregateExpr, plan.schema())
}
```

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
