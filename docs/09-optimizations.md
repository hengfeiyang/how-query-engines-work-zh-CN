# 查询优化

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `optimizer` 模块中找到。_

我们现在有了功能性的查询计划，但是我们依赖于终端用户以高效的方式构建这些计划。例如，我们期望用户能够构建出尽早进行过滤操作的计划，特别是在联表之前，因为这可以限制需要处理的数据量。

现在是实现一个简单的基于规则的查询优化器的好时机，它可以重新排列查询计划使其更加高效。

一旦我们在第十一章开始支持 SQL，这一点将变得更加重要，因为 SQL 语言仅定义查询应如何工作，并不总是允许用户指定运算符和表达式的计算顺序。

## 基于规则的优化 Rule-Based Optimizations

基于规则的优化是一种将常识性优化应用于查询计划的简单实用的方法。尽管基于规则的优化也可以应用于物理计划，但这些优化通常在创建物理计划之前针对逻辑计划执行。

优化的工作原理是使用访问者模式遍历逻辑计划，并创建计划中每个步骤的副本并应用任何必要的修改。这是一个比在执行计划时尝试改变状态要简单得多的设计，并且与喜欢不可变状态的函数式编程风格非常一致。

我们将使用以下接口来表示优化器规则。 

```kotlin
interface OptimizerRule {
  fun optimize(plan: LogicalPlan) : LogicalPlan
}
```

现在我们将了解大多数查询引擎实现的一些常见优化规则。

### 映射下推 Projection Push-Down

映射下推规则的目标是在从磁盘读取数据之后和查询执行的其他阶段之前尽快过滤掉列，以减少操作符之间保留在内存中（以及在分布式场景下可能通过网络传输）的数据量。

为了知道查询中引用了哪些列，我们必须编写递归代码来检查表达式并构建列列表。

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

有了这个实用的代码，我们就可以继续实施优化器规则。请注意，对于 `Projection`、`Selection` 和 `Aggregate` 计划，我们正在构建列名列表，但当我们到达 `Scan`（它是一个叶节点）时，我们将其替换为在查询中其他地方使用的列名列表的扫描版本。 

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

给定这个输入逻辑计划：

```
Projection: #id, #first_name, #last_name
  Filter: #state = 'CO'
    Scan: employee; projection=None
```

该优化器规则会将其转换为以下计划。

```
Projection: #id, #first_name, #last_name
  Filter: #state = 'CO'
    Scan: employee; projection=[first_name, id, last_name, state]
```

### 断言下推 Predicate Push-Down

断言下推优化的目的是在查询中尽早过滤掉行，以避免冗余处理。考虑以下内容，联接 `employee` 表和 `dept` 表，然后过滤位于科罗拉多州（Colorado）的员工。

```
Projection: #dept_name, #first_name, #last_name
  Filter: #state = 'CO'
    Join: #employee.dept_id = #dept.id
      Scan: employee; projection=[first_name, id, last_name, state]
      Scan: dept; projection=[id, dept_name]
```

该查询将产生正确的结果，但会有执行所有员工（而不仅仅是那些位于科罗拉多州的员工）联接操作所带来的开销。断言下推规则会将过滤器下推到联接中，如下查询计划所示。

```
Projection: #dept_name, #first_name, #last_name
  Join: #employee.dept_id = #dept.id
    Filter: #state = 'CO'
      Scan: employee; projection=[first_name, id, last_name, state]
    Scan: dept; projection=[id, dept_name]
```

联表现在将仅处理一部分员工，从而获得更好的性能。

### 消除公共子表达式 Eliminate Common Subexpressions

给定一个查询如 `SELECT sum(price * qty) as total_price, sum(price * qty * tax_rate) as total_tax FROM ...`，我们可以看到表达式 `price * qty` 出现了两次，我们可以选择重写计划只计算一次，而不是执行两次计算。。

原始计划：

```
Projection: sum(#price * #qty), sum(#price * #qty * #tax)
  Scan: sales
```

优化后的计划：

```
Projection: sum(#_price_mult_qty), sum(#_price_mult_qty * #tax)
  Projection: #price * #qty as _price_mult_qty
    Scan: sales
```

### 将相关子查询转换为联表 Converting Correlated Subqueries to Joins

给定一个查询如 `SELECT id FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`，一个简单的实现可能是扫描 `foo` 中的所有行，然后对 `bar` 中查找每一行。这将非常低效，所以查询引擎通常会把相关子查询转化为联表操作。这也被称为子查询去关联。

此查询可以改写为 `SELECT foo.id FROM foo JOIN bar ON foo.id = bar.id`.

```
Projection: foo.id
  LeftSemi Join: foo.id = bar.id
    TableScan: foo projection=[id]
    TableScan: bar projection=[id]
```

如果查询被修改为使用 `NOT EXISTS` 而不是 `EXISTS`，那么查询计划将使用 `LeftAnti` 而不是 `LeftSemi` 联接。

```
Projection: foo.id
  LeftAnti Join: foo.id = bar.id
    TableScan: foo projection=[id]
    TableScan: bar projection=[id]
```

## 基于成本的优化 Cost-Based Optimizations

基于成本的优化是指利用底层数据的统计信息来确定执行特定查询的成本，然后通过寻找成本较低的执行计划来选择最佳执行计划的优化规则。一个好的例子是根据基础表的大小选择要使用的联接算法，或者选择联接表的顺序。

基于成本的优化的一个主要缺点是它们依赖于相关底层数据的准确且详细的统计数据的可用性。此类统计信息通常包括每列统计信息，例如空值的数量、不同值的数量、最小值和最大值以及显示列内值分布的直方图。直方图对于能够检测诸如 `state = 'CA'` 之类的断言可能产生比 `state = 'WY'` 更多的行至关重要，比如：（加利福尼亚州是美国人口最多的州，有 3900 万居民，而怀俄明州是人口最少的州，不到 100 万居民）。

当处理 Orc 或 Parquet 等文件格式时，其中一些统计信息是可用的，但通常需要运行一个进程来构建这些统计信息，而当处理 TB 级别的数据时，这可能会令人望而却步，并且得不偿失，特别是对于临时查询。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
