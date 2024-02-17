# 逻辑计划和表达式

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `logical-plan` 模块中找到。_

逻辑计划表示具有已知结构的关系（一组元组）。每个逻辑计划可以有零个或多个逻辑计划作为输入。逻辑计划可以很方便地公开其子计划，以便可以使用访问者模式来遍历该计划。

```kotlin
interface LogicalPlan {
  fun schema(): Schema
  fun children(): List<LogicalPlan>
}
```

## 打印逻辑计划

能够以人类可读形式打印逻辑计划对于帮助调试非常重要。逻辑计划通常打印为带有缩进子节点的分层结构。

我们可以实现一个简单的递归辅助函数来格式化逻辑计划。

```kotlin
fun format(plan: LogicalPlan, indent: Int = 0): String {
  val b = StringBuilder()
  0.rangeTo(indent).forEach { b.append("\t") }
  b.append(plan.toString()).append("\n")
  plan.children().forEach { b.append(format(it, indent+1)) }
  return b.toString()
}
```

以下是使用此方法格式化逻辑计划的一个例子。

```
Projection: #id, #first_name, #last_name, #state, #salary
  Filter: #state = 'CO'
    Scan: employee.csv; projection=None
```

## 序列化

有时希望能够序列化查询计划，以便可以轻松地将它们转移到另一个进程。最好尽早添加序列化，以防止意外引用无法序列化的数据结构（如文件句柄或数据库连接）。

一种方法是使用实​​现语言的默认机制将数据结构序列化/反序列化为 JSON 等格式。在 Java 中可以使用 Jackson 库，Kotlin 有 `kotlinx.serialization` 库，Rust 则有 serde crate等。

另一种选择可能是定义一种与语言无关的序列化格式，如 Avro、Thrift 或 Protocol Buffers，然后编写代码在此格式和特定于语言实现之间进行转换。

自本书第一版发来以来，出现了一个名为 [“substrait”](https://substrait.io/) 的新标准，其目标是为关系代数提供跨语言序列化。我对这个项目感到很兴奋，并预测它将成为表示查询计划的事实上的标准，并开辟许多集成的可能性。例如，可以使用成熟的基于 Java 的查询计划程序（例如 Apache Calcite），以 Substrait 格式序列化计划，然后在以较低级别语言（例如 C++ 或 Rust）实现的查询引擎中执行计划。欲了解更多信息，请访问 https://substrait.io/。

## 逻辑表达式

表达式这一概念是查询计划的基本构建块之一，其可以在运行时对数据进行求值。

以下是查询引擎中通常支持的一些表达式示例。

| 表达式                 | 示例                                |
|-----------------------|------------------------------------|
| Literal Value         | "hello", 12.34                     |
| Column Reference      | user_id, first_name, last_name     |
| Math Expression       | salary * state_tax                 |
| Comparison Expression | x >= y                             |
| Boolean Expression    | birthday = today() AND age >= 21   |
| Aggregate Expression  | MIN(salary), MAX(salary), SUM(salary), AVG(salary), COUNT(*) |
| Scalar Function       | CONCAT(first_name, " ", last_name) |
| Aliased Expression    | salary * 0.02 AS pay_increase      |

当然，所有这些表达式都可以组合形成深层嵌套的表达式树。表达式求值是递归编程的典型案例。

在规划查询时，我们需要了解有关表达式输出的一些基本元数据。具体来说，我们需要为表达式指定一个名称，以便其他表达式可以引用它，并且我们需要知道表达式求值时将生成的值的数据类型，以便我们可以验证查询计划是否有效。例如，如果我们有一个表达式 `a + b` ，那么只有当 `a` 和 `b` 都是数值类型时才有效。

还需注意，表达式的数据类型可能取决于输入数据。例如，列引用将具有它所引用的列的数据类型，但比较表达式始终返回布尔值。

```kotlin
interface LogicalExpr {
  fun toField(input: LogicalPlan): Field
}
```

### 列式表达式

`Column` 表达式仅代表对一个命名列的引用。该表达式的元数据是通过在输入中查找指定列并返回该列的元数据而派生的。请注意，此处的术语 `Column` 指的是由输入逻辑计划生成的列，并且可以表示数据源中的列，或者它可以表示针对其他输入求值的表达式的结果。

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

### 字面量表达式

我们需要能够将字面量表示为表达式的能力，以便我们可以编写像 `salary * 0.05` 这样的表达式。

这是一个用于字符串字面量的表达式示例。

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

这是一个用于长整型字面值的表达式示例。

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

### 二元表达式

二元表达式简单来说就是只接受两个输入的表达式。我们将实现三类二元表达式，即比较表达式、布尔表达式和数学表达式。因为所有这些的字符串表示形式都是相同的，我们可以使用一个公共基类来提供`toString`方法。变量 “l” 和 “r” 分别指左输入和右输入。

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

比如 `=` 或 `<` 这样的比较表达式会比较两个相同数据类型的值，并返回一个布尔值。我们还需要实现布尔运算符 `AND` 和 `OR` ，它们也接受两个参数并产生一个布尔结果，因此我们也可以为这些运算符使用一个公共基类。

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

该基类提供了一种简洁的方式来实现具体的比较表达式。

### 比较表达式

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

### 布尔表达式

该基类还提供了一种简洁的方式来实现具体的布尔逻辑表达式。

```kotlin
/** Logical AND */
class And(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("and", "AND", l, r)

/** Logical OR */
class Or(l: LogicalExpr, r: LogicalExpr)
    : BooleanBinaryExpr("or", "OR", l, r)
```

### 数学表达式

数学表达式是二元表达式的另一种特殊形式。数学表达式通常对相同数据类型的值进行运算并产生相同数据类型的结果。

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

### 聚合表达式

聚合表达式对输入表达式执行聚合函数，如 `MIN`、`MAX`、`COUNT`、`SUM` 或者 `AVG` 等。

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

对于聚合数据类型与输入类型相同的聚合表达式，我们可以简单地扩展这个基类。

```kotlin
class Sum(input: LogicalExpr) : AggregateExpr("SUM", input)
class Min(input: LogicalExpr) : AggregateExpr("MIN", input)
class Max(input: LogicalExpr) : AggregateExpr("MAX", input)
class Avg(input: LogicalExpr) : AggregateExpr("AVG", input)
```

对于数据类型不依赖于输入类型的聚合表达式，我们需要重写 `toField` 方法。例如，“COUNT” 聚合表达式无论计数值的数据类型为何，总是产生一个整数结果。

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

## 逻辑计划

有了逻辑表达式，逻辑表达式就位后，我们现在可以为查询引擎支持的各种转换实现逻辑计划。

### 扫描

`扫描（Scan）` 逻辑计划表示根据可选 `映射（projection）`从一个 `数据源（DataSource）` 中获取数据。在我们查询引擎中 `扫描（Scan）` 逻辑计划是唯一没有其他逻辑计划作为输入的逻辑计划，它是查询树中的叶节点。

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

### 映射

`映射（Projection）` 逻辑计划对其输入应用映射。一个映射是对输入数据进行求值的一系列表达式列表。有时候这是一个简单的字段列表, 比如 `SELECT a, b, c FROM foo`, 但也可能包括任何其他支持的表达式，一个更复杂例子可能是： `SELECT (CAST(a AS float) * 3.141592)) AS my_float FROM foo`。

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

### 筛选（也称为过滤器）

`筛选（Selection）` 逻辑计划通过应用过滤器表达式来确定应在其输出中选择（包含）哪些行。这在 SQL 中用 `WHERE` 子句表示。一个简单例子可能是：`SELECT * FROM foo WHERE a > 5`，过滤器表达式需要求值出一个布尔结果。

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

### 聚合

`聚合（Aggregate）` 逻辑计划远比 `映射（Projection）`、`筛选（Selection）` 或者 `扫描（Scan）` 复杂，并且能够计算出底层数据如 最小值、最大值、平均值 和 总和 等聚集信息。聚合通常按其他列（或表达式）进行分组。一个简单例子可能是：`SELECT job_title, AVG(salary) FROM employee GROUP BY job_title`。

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

请注意，在此实现中，聚合计划的输出是通过分组列和聚合表达式来组织的。通常需要将聚合逻辑计划包装在映射中，以便按照原始查询中的请求顺序返回列。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
