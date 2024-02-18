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

### 列表达式 Column Expressions

逻辑列表达式按名称引用列，但物理表达式使用列索引来提高性能，因此查询规划器需要执行从列名到列索引的转换，并在列名无效时抛出异常。

这个简化的示例查找第一个匹配的列名称，并没有检查是否有多个匹配的列，这应该是一个错误条件。

```kotlin
is Column -> {
  val i = input.schema().fields.indexOfFirst { it.name == expr.name }
  if (i == -1) {
    throw SQLException("No column named '${expr.name}'")
  }
  ColumnExpression(i)
```

### 字面量表达式 Literal Expressions

字面量的物理表达式很简单，从逻辑表达式到物理表达式的映射很简单，因为我们需要复制字面值。

```kotlin
is LiteralLong -> LiteralLongExpression(expr.n)
is LiteralDouble -> LiteralDoubleExpression(expr.n)
is LiteralString -> LiteralStringExpression(expr.str)
```

### 二元表达式 Binary Expressions

要为二元表达式创建物理表达式，我们首先需要为左右输入创建物理表达式，然后需要创建特定的物理表达式。

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

我们需要实现一个递归函数来遍历逻辑计划树并将其转换为物理计划，使用前面描述的转换表达式的相同模式。

```kotlin
fun createPhysicalPlan(plan: LogicalPlan) : PhysicalPlan {
  return when (plan) {
    is Scan -> ...
    is Selection -> ...
    ...
}
```

### 扫描 Scan

转换扫描计划只需复制数据源引用和逻辑计划的映射。

```kotlin
is Scan -> ScanExec(plan.dataSource, plan.projection)
```

### 映射 Projection

转换映射有两个步骤。首先，我们需要为映射的输入创建一个物理计划，然后我们需要将映射的逻辑表达式转换为物理表达式。

```kotlin
is Projection -> {
  val input = createPhysicalPlan(plan.input)
  val projectionExpr = plan.expr.map { createPhysicalExpr(it, plan.input) }
  val projectionSchema = Schema(plan.expr.map { it.toField(plan.input) })
  ProjectionExec(input, projectionSchema, projectionExpr)
}
```

### 筛选（也称为过滤器） Selection (also known as Filter)

`Selection` 的查询规划步骤与 `Projection` 非常相似。

```kotlin
is Selection -> {
  val input = createPhysicalPlan(plan.input)
  val filterExpr = createPhysicalExpr(plan.expr, plan.input)
  SelectionExec(input, filterExpr)
}
```

### 聚合 Aggregate

聚合查询的查询规划步骤涉及计算定义可选 分组键（grouping keys）的表达式和计算作为聚合函数的输入的表达式，然后创建物理聚合表达式。

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
