# 物理计划和表达式

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `physical-plan` 模块中找到。_

第五章定义的逻辑计划指明了要做什么，但没有说明如何去做，将逻辑计划和物理计划分开是一个好习惯，尽管可以将它们结合起来以降低复杂性。

将逻辑计划和物理计划分开的原因之一是，有时可以有多种方法来执行特定操作，这意味着逻辑计划和物理计划之间存在一对多的关系。 

例如，单进程与分布式执行、CPU 与 GPU 执行可能有不同的物理计划。

此外，诸如 `聚合（Aggregate）` 和 `联表（Join）` 之类的操作可以通过各种算法实现，并且具有不同的性能权衡。当聚合已经按 分组字段（grouping keys）排序的数据时，使用 Group Aggregate（也称为Sort Aggregate）非常有效，它一次只需要保存一组 分组字段（grouping keys）的状态，并且可以在一组数据处理完毕立即给出结果。如果数据未排序，则通常使用哈希聚合，哈希聚合通过对键进行分组来维护一个 HashMap 累加器。 

联表则有更广泛的算法选择，包括 嵌套循环连接（Nested Loop Join）、排序合并连接（Sort-Merge Join）和 哈希连接（Hash Join）。

物理计划返回 记录批次（record batches）的迭代器。

```kotlin
interface PhysicalPlan {
  fun schema(): Schema
  fun execute(): Sequence<RecordBatch>
  fun children(): List<PhysicalPlan>
}
```

## 物理表达式

我们已经定义了逻辑计划中引用的逻辑表达式，但现在需要实现包含代码的物理表达式类，以在运行时计算表达式。

每个逻辑表达式可以有多个物理表达式实现。例如，对于将两个数字相加的逻辑表达式 `AddExpr`，我们可以有一种使用 CPU 的实现和一种使用 GPU 的实现。查询规划器可以根据运行代码的服务器的硬件能力来选择使用哪一个。

物理表达式是针对 记录批次（record batches）进行计算的，其结果是列。

以下是我们将用来表示物理表达式的接口。

```kotlin
interface Expression {
  fun evaluate(input: RecordBatch): ColumnVector
}
```

### 列表达式

`Column` 表达式简单地求值为对正在处理的 `RecordBatch` 中的 `ColumnVector` 的引用。`Column` 的逻辑表达式通过名称引用输入，这对编写查询来说是用户友好的，但对于物理表达式，我们希望避免每次评估表达式时都进行名称查找的成本，因此它改为通过索引引用列。

```kotlin
class ColumnExpression(val i: Int) : Expression {

  override fun evaluate(input: RecordBatch): ColumnVector {
    return input.field(i)
  }

  override fun toString(): String {
    return "#$i"
  }
}
```

### 字面量表达式

字面量表达式的物理实现就是一个包装在类中的字面值，该类实现了相应特性并为列中每个索引提供相同的值。

```kotlin
class LiteralValueVector(
    val arrowType: ArrowType,
    val value: Any?,
    val size: Int) : ColumnVector {

  override fun getType(): ArrowType {
    return arrowType
  }

  override fun getValue(i: Int): Any? {
    if (i<0 || i>=size) {
      throw IndexOutOfBoundsException()
    }
    return value
  }

  override fun size(): Int {
    return size
  }

}
```

有了这个类，我们就可以为每种数据类型的字面量表达式创建物理表达式。

```kotlin
class LiteralLongExpression(val value: Long) : Expression {
  override fun evaluate(input: RecordBatch): ColumnVector {
    return LiteralValueVector(ArrowTypes.Int64Type,
                              value,
                              input.rowCount())
  }
}

class LiteralDoubleExpression(val value: Double) : Expression {
  override fun evaluate(input: RecordBatch): ColumnVector {
    return LiteralValueVector(ArrowTypes.DoubleType,
                              value,
                              input.rowCount())
  }
}

class LiteralStringExpression(val value: String) : Expression {
  override fun evaluate(input: RecordBatch): ColumnVector {
    return LiteralValueVector(ArrowTypes.StringType,
                              value.toByteArray(),
                              input.rowCount())
  }
}
```

### 二元表达式

对于二元表达式，我们需要计算左右输入表达式，然后根据这些输入值计算特定的二元运算符，因此我们可以提供一个基类来简化每个运算符的实现。

```kotlin
abstract class BinaryExpression(val l: Expression, val r: Expression) : Expression {
  override fun evaluate(input: RecordBatch): ColumnVector {
    val ll = l.evaluate(input)
    val rr = r.evaluate(input)
    assert(ll.size() == rr.size())
    if (ll.getType() != rr.getType()) {
      throw IllegalStateException(
          "Binary expression operands do not have the same type: " +
          "${ll.getType()} != ${rr.getType()}")
    }
    return evaluate(ll, rr)
  }

  abstract fun evaluate(l: ColumnVector, r: ColumnVector) : ColumnVector
}
```

### 比较表达式

比较表达式只是简单地比较两个输入列中的所有值并生成包含结果的新列（位向量 bit vector）。

以下是相等运算符的一个例子。 

```kotlin
class EqExpression(l: Expression,
                   r: Expression): BooleanExpression(l,r) {

  override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Boolean {
    return when (arrowType) {
      ArrowTypes.Int8Type -> (l as Byte) == (r as Byte)
      ArrowTypes.Int16Type -> (l as Short) == (r as Short)
      ArrowTypes.Int32Type -> (l as Int) == (r as Int)
      ArrowTypes.Int64Type -> (l as Long) == (r as Long)
      ArrowTypes.FloatType -> (l as Float) == (r as Float)
      ArrowTypes.DoubleType -> (l as Double) == (r as Double)
      ArrowTypes.StringType -> toString(l) == toString(r)
      else -> throw IllegalStateException(
          "Unsupported data type in comparison expression: $arrowType")
    }
  }
}
```

### 数学表达式

数学表达式的实现与比较表达式的代码非常相似。一个基类可以用于所有数学表达式。


```kotlin
abstract class MathExpression(l: Expression,
                              r: Expression): BinaryExpression(l,r) {

  override fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector {
    val fieldVector = FieldVectorFactory.create(l.getType(), l.size())
    val builder = ArrowVectorBuilder(fieldVector)
    (0 until l.size()).forEach {
      val value = evaluate(l.getValue(it), r.getValue(it), l.getType())
      builder.set(it, value)
    }
    builder.setValueCount(l.size())
    return builder.build()
  }

  abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Any?
}
```

下面是扩展此基类的特定数学表达式的一个例子。

```kotlin
class AddExpression(l: Expression,
                    r: Expression): MathExpression(l,r) {

  override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType) : Any? {
      return when (arrowType) {
        ArrowTypes.Int8Type -> (l as Byte) + (r as Byte)
        ArrowTypes.Int16Type -> (l as Short) + (r as Short)
        ArrowTypes.Int32Type -> (l as Int) + (r as Int)
        ArrowTypes.Int64Type -> (l as Long) + (r as Long)
        ArrowTypes.FloatType -> (l as Float) + (r as Float)
        ArrowTypes.DoubleType -> (l as Double) + (r as Double)
        else -> throw IllegalStateException(
            "Unsupported data type in math expression: $arrowType")
      }
  }

  override fun toString(): String {
    return "$l+$r"
  }
}
```

### 聚合表达式

到目前为止，我们所研究的表达式都是从每个批次中的一列或多列输入生成一个输出列。聚合表达式会更复杂，因为它们聚合多批数据中的值，然后生成一个最终值，因此我们需要引入累加器的概念，每个聚合表达式的物理表示需要知道如何为查询引擎生成适当的累加器来传递输入数据。
 
以下是表示聚合表达式和累加器的主要接口。

```kotlin
interface AggregateExpression {
  fun inputExpression(): Expression
  fun createAccumulator(): Accumulator
}

interface Accumulator {
  fun accumulate(value: Any?)
  fun finalValue(): Any?
}
```

聚合表达式 `Max` 的实现将会生成一个特定的 最大值累加器（MaxAccumulator）。

```kotlin
class MaxExpression(private val expr: Expression) : AggregateExpression {

  override fun inputExpression(): Expression {
    return expr
  }

  override fun createAccumulator(): Accumulator {
    return MaxAccumulator()
  }

  override fun toString(): String {
    return "MAX($expr)"
  }
}
```

以下是 MaxAccumulator 的实现示例。

```kotlin
class MaxAccumulator : Accumulator {

  var value: Any? = null

  override fun accumulate(value: Any?) {
    if (value != null) {
      if (this.value == null) {
        this.value = value
      } else {
        val isMax = when (value) {
          is Byte -> value > this.value as Byte
          is Short -> value > this.value as Short
          is Int -> value > this.value as Int
          is Long -> value > this.value as Long
          is Float -> value > this.value as Float
          is Double -> value > this.value as Double
          is String -> value > this.value as String
          else -> throw UnsupportedOperationException(
            "MAX is not implemented for data type: ${value.javaClass.name}")
        }

        if (isMax) {
          this.value = value
        }
      }
    }
  }

  override fun finalValue(): Any? {
    return value
  }
}
```

## 物理计划

有了物理表达式之后，我们现在可以为查询引擎将支持各种转换实现物理计划了。

### 扫描

`Scan` 执行计划只是委派给数据源，传入一个 映射（Projection）来限制加载到内存中的列。不执行附加逻辑。

```kotlin
class ScanExec(val ds: DataSource, val projection: List<String>) : PhysicalPlan {

  override fun schema(): Schema {
    return ds.schema().select(projection)
  }

  override fun children(): List<PhysicalPlan> {
    // Scan is a leaf node and has no child plans
    return listOf()
  }

  override fun execute(): Sequence<RecordBatch> {
    return ds.scan(projection);
  }

  override fun toString(): String {
    return "ScanExec: schema=${schema()}, projection=$projection"
  }
}
```

### 映射

映射（Projection）执行计划只是根据输入列评估映射（Projection）表达式，然后生成包含派生列的记录批次（record batch）。请注意，对于按名称引用现有列的映射（Projection）表达式的情况，派生列只是对输入列的指针或引用，因此不会复制底层数据。

```kotlin
class ProjectionExec(
    val input: PhysicalPlan,
    val schema: Schema,
    val expr: List<Expression>) : PhysicalPlan {

  override fun schema(): Schema {
    return schema
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun execute(): Sequence<RecordBatch> {
    return input.execute().map { batch ->
      val columns = expr.map { it.evaluate(batch) }
        RecordBatch(schema, columns)
      }
  }

  override fun toString(): String {
    return "ProjectionExec: $expr"
  }
}
```

### 筛选（也称为过滤器）

筛选执行计划是第一个重要的计划，因为它具有条件逻辑来确定输入记录批次中的哪些行应包含在输出批次中。

对于每个输入批次，筛选表达式被执行以返回一个位向量（bit vector），其中包含表示表达式布尔结果的位，每行一位。然后使用该位向量过滤输入列以生成新的输出列。这是一种简单实现，可以针对位向量包含全 1 或全 0 的情况进行优化，以避免将数据复制到新向量的开销。

```kotlin
class SelectionExec(
    val input: PhysicalPlan,
    val expr: Expression) : PhysicalPlan {

  override fun schema(): Schema {
    return input.schema()
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun execute(): Sequence<RecordBatch> {
    val input = input.execute()
    return input.map { batch ->
      val result = (expr.evaluate(batch) as ArrowFieldVector).field as BitVector
      val schema = batch.schema
      val columnCount = batch.schema.fields.size
      val filteredFields = (0 until columnCount).map {
          filter(batch.field(it), result)
      }
      val fields = filteredFields.map { ArrowFieldVector(it) }
      RecordBatch(schema, fields)
    }

  private fun filter(v: ColumnVector, selection: BitVector) : FieldVector {
    val filteredVector = VarCharVector("v",
                                       RootAllocator(Long.MAX_VALUE))
    filteredVector.allocateNew()

    val builder = ArrowVectorBuilder(filteredVector)

    var count = 0
    (0 until selection.valueCount).forEach {
      if (selection.get(it) == 1) {
        builder.set(count, v.getValue(it))
        count++
      }
    }
    filteredVector.valueCount = count
    return filteredVector
  }
}
```

### 哈希聚合

哈希聚合计划（HashAggregate）比以前的计划更复杂，因为它必须处理所有传入批次并维护累加器的 HashMap 并更新正在处理的每一行的累加器。最后，利用累加器结果创建一个包含聚合查询结果的记录批次（record batch）。

```kotlin
class HashAggregateExec(
    val input: PhysicalPlan,
    val groupExpr: List<PhysicalExpr>,
    val aggregateExpr: List<PhysicalAggregateExpr>,
    val schema: Schema) : PhysicalPlan {

  override fun schema(): Schema {
    return schema
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun toString(): String {
    return "HashAggregateExec: groupExpr=$groupExpr, aggrExpr=$aggregateExpr"
  }

  override fun execute(): Sequence<RecordBatch> {
    val map = HashMap<List<Any?>, List<Accumulator>>()

    // for each batch from the input executor
    input.execute().iterator().forEach { batch ->

    // evaluate the grouping expressions
    val groupKeys = groupExpr.map { it.evaluate(batch) }

    // evaluate the expressions that are inputs to the aggregate functions
    val aggrInputValues = aggregateExpr.map {
        it.inputExpression().evaluate(batch)
    }

    // for each row in the batch
    (0 until batch.rowCount()).forEach { rowIndex ->
      // create the key for the hash map
      val rowKey = groupKeys.map {
      val value = it.getValue(rowIndex)
      when (value) {
        is ByteArray -> String(value)
        else -> value
      }
    }

    // get or create accumulators for this grouping key
    val accumulators = map.getOrPut(rowKey) {
        aggregateExpr.map { it.createAccumulator() }
    }

    // perform accumulation
    accumulators.withIndex().forEach { accum ->
      val value = aggrInputValues[accum.index].getValue(rowIndex)
      accum.value.accumulate(value)
    }

    // create result batch containing final aggregate values
    val allocator = RootAllocator(Long.MAX_VALUE)
    val root = VectorSchemaRoot.create(schema.toArrow(), allocator)
    root.allocateNew()
    root.rowCount = map.size

    val builders = root.fieldVectors.map { ArrowVectorBuilder(it) }

    map.entries.withIndex().forEach { entry ->
      val rowIndex = entry.index
      val groupingKey = entry.value.key
      val accumulators = entry.value.value
      groupExpr.indices.forEach {
        builders[it].set(rowIndex, groupingKey[it])
      }
      aggregateExpr.indices.forEach {
        builders[groupExpr.size+it].set(rowIndex, accumulators[it].finalValue())
      }
    }

    val outputBatch = RecordBatch(schema, root.fieldVectors.map {
       ArrowFieldVector(it)
    })

    return listOf(outputBatch).asSequence()
  }

}
```

### 联表

顾名思义，Join 运算符连接两个关系中的行。有许多不同类型的具有不同的语义 Join：

- `[INNER] JOIN`: 这是最常用的联表类型，创建一个包含来自左右输入的行的新关系行。当连接表达式仅包含左右输入的列之间的相等比较时，该连接称为“等连接”。等连接的一个例子是 `SELECT * FROM customer JOIN orders ON customer.id = order.customer_id`。
- `LEFT [OUTER] JOIN`: 左外连接生成包含左输入中所有值的行，并包含匹配条件的右输入中的行。如果右侧不匹配，则为右侧列生成空值。
- `RIGHT [OUTER] JOIN`: 这与左连接相反。返回右输入的所有行，并包含匹配条件的左输入中的行。如果左侧不匹配，则为左侧列生成空值。
- `SEMI JOIN`: 半连接类似于左连接，但它只返回左输入中与右输入匹配的行，不会从右输入返回任何数据。并非所有 SQL 实现都显式支持半连接，它们通常被编写为子查询。半连接的一个例子是 `SELECT id FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`。
- `ANTI JOIN`: 反连接与半连接相反，仅返回左输入中与右输入不匹配的行。反连接的一个例子是 `SELECT id FROM foo WHERE NOT EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`。
- `CROSS JOIN`: 交叉连接返回左输入中的行与右输入中的行的所有可能组合。如果左侧输入包含 100 行，右侧输入包含 200 行，则将返回 20,000 行。这称为笛卡尔积。

KQuery 尚未实现 Join 运算符。

### 子查询

子查询是查询中的查询。它们可以是相关的，也可以是不相关的（涉及或不涉及其他关系的联接）。当子查询返回单一值时，它被称为标量子查询。

#### 标量子查询

标量子查询返回单个值，并且可以在许多可以使用字面量的 SQL 表达式中使用。

*下面是相关性标量子查询的一个例子：*

`SELECT id, name, (SELECT count(*) FROM orders WHERE customer_id = customers.id) AS num_orders FROM customers`

*下面是无相关性标量子查询的一个例子：*

`SELECT * FROM orders WHERE total > (SELECT avg(total) FROM sales WHERE customer_state = 'CA')`

相关性子查询在执行之前被转换为连接（这将在第 9 章中解释）。

无相关性的查询可以单独执行，并且结果值可以替换到上级查询中。

#### EXISTS 和 IN 子查询

`EXISTS` 和 `IN` 表达式（及其否定形式，`NOT EXISTS` 和 `NOT IN`）可以用来创建半连接和反连接。

下面是一个半连接的示例，它选择左侧关系（`foo`）中所有与子查询返回的行匹配的行。

`SELECT id FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`

相关性子查询通常在逻辑计划优化期间转换为 联表（这在第 9 章中进行了解释）

KQuery 尚未实现子查询。

## 创建物理计划

有了物理计划，下一步是构建一个查询规划器，从逻辑计划创建物理计划，我们将在下一章中讲解。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
