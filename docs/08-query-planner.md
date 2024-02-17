# 查询规划器

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `query-planner` 模块中找到。_

我们已经定义了逻辑和物理查询计划，现在我们需要一个可以将逻辑计划转换为物理计划的查询计划器。

查询规划器可以基于配置选项或基于目标平台的硬件能力来选择不同的物理计划。例如，查询可以在 CPU 或 GPU 上、单个节点上或者分布在集群中执行。

## 转换逻辑表达式

第一步是定义一种方法，以递归方式将逻辑表达式转换为物理表达式。以下代码示例演示了基于 switch 语句的实现，并展示了如何把具有两个输入表达式的二元表达式使用递归的方法来转换这些输入。这种方法遍历整个逻辑表达式树并创建相应的物理表达式树。

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

以下部分将解释每种类型表达式的实现。

### 列表达式（Column Expressions）


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

### 字面量表达式（Literal Expressions）

The physical expressions for literal values are straightforward, and the mapping from logical to physical expression is trivial because we need to copy the literal value over.

```kotlin
is LiteralLong -> LiteralLongExpression(expr.n)
is LiteralDouble -> LiteralDoubleExpression(expr.n)
is LiteralString -> LiteralStringExpression(expr.str)
```

### 二元表达式（Binary Expressions）

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

## 转换逻辑计划

We need to implement a recursive function to walk the logical plan tree and translate it into a physical plan, using the same pattern described earlier for translating expressions.

```kotlin
fun createPhysicalPlan(plan: LogicalPlan) : PhysicalPlan {
  return when (plan) {
    is Scan -> ...
    is Selection -> ...
    ...
}
```

### 扫描（Scan）

Translating the Scan plan simply requires copying the data source reference and the logical plan's projection.

```kotlin
is Scan -> ScanExec(plan.dataSource, plan.projection)
```

### 映射（Projection）

There are two steps to translating a projection. First, we need to create a physical plan for the projection's input, and then we need to convert the projection's logical expressions to physical expressions.

```kotlin
is Projection -> {
  val input = createPhysicalPlan(plan.input)
  val projectionExpr = plan.expr.map { createPhysicalExpr(it, plan.input) }
  val projectionSchema = Schema(plan.expr.map { it.toField(plan.input) })
  ProjectionExec(input, projectionSchema, projectionExpr)
}
```

### 筛选（也称为过滤器）（Selection (also known as Filter)）

The query planning step for `Selection` is very similar to `Projection`.

```kotlin
is Selection -> {
  val input = createPhysicalPlan(plan.input)
  val filterExpr = createPhysicalExpr(plan.expr, plan.input)
  SelectionExec(input, filterExpr)
}
```

### 聚合（Aggregate）

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
