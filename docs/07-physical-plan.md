# Physical Plans & Expressions

_The source code discussed in this chapter can be found in the `physical-plan` module of the[ KQuery project](https://github.com/andygrove/how-query-engines-work)._

The logical plans defined in chapter five specify what to do but not how to do it, and it is good practice to have separate logical and physical plans, although it is possible to combine them to reduce complexity.

One reason to keep logical and physical plans separate is that sometimes there can be multiple ways to execute a particular operation, meaning that there is a one-to-many relationship between logical plans and physical plans.

For example, there could be separate physical plans for single-process versus distributed execution, or CPU versus GPU execution.

Also, operations such as `Aggregate` and `Join` can be implemented with a variety of algorithms with different performance trade-offs. When aggregating data that is already sorted by the grouping keys, it is efficient to use a Group Aggregate (also known as a Sort Aggregate) which only needs to hold state for one set of grouping keys at a time and can emit a result as soon as one set of grouping keys ends. If the data is not sorted, then a Hash Aggregate is typically used. A Hash Aggregate maintains a HashMap of accumulators by grouping keys.

Joins have an even wider variety of algorithms, including Nested Loop Join, Sort-Merge Join, and Hash Join.

Physical plans return iterators over record batches.

```kotlin
interface PhysicalPlan {
  fun schema(): Schema
  fun execute(): Sequence<RecordBatch>
  fun children(): List<PhysicalPlan>
}
```

## Physical Expressions

We have defined logical expressions that are referenced in the logical plans, but we now need to implement physical expression classes containing the code to evaluate the expressions at runtime.

There could be multiple physical expression implementations for each logical expression. For example, for the logical expression `AddExpr` that adds two numbers, we could have one implementation that uses the CPU and one that uses the GPU. The query planner could choose which one to use based on the hardware capabilities of the server that the code is running on.

Physical expressions are evaluated against record batches and the results are columns.

Here is the interface that we will use to represent physical expressions.

```kotlin
interface Expression {
  fun evaluate(input: RecordBatch): ColumnVector
}
```

## Column Expressions

The `Column` expression simply evaluates to a reference to the `ColumnVector` in the `RecordBatch` being processed. The logical expression for `Column` references inputs by name, which is user-friendly for writing queries, but for the physical expression we want to avoid the cost of name lookups every time the expression is evaluated, so it references columns by index instead.

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

## Literal Expressions

The physical implementation of a literal expression is simply a literal value wrapped in a class that implements the appropriate trait and provides the same value for every index in a column.

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

With this class in place, we can create our physical expressions for literal expressions of each data type.

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

## Binary Expressions

For binary expressions, we need to evaluate the left and right input expressions and then evaluate the specific binary operator against those input values, so we can provide a base class to simplify the implementation for each operator.

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

## Comparison Expressions

The comparison expressions simply compare all values in the two input columns and produce a new column (a bit vector) containing the results.

Here is an example for the equality operator.

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

## Math Expressions

The implementation for math expressions is very similar to the code for comparison expressions. A base class is used for all math expressions.

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

Here is an example of a specific math expression extending this base class.

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

## Aggregate Expressions

The expressions we have looked at so far produce one output column from one or more input columns in each batch. Aggregate expressions are more complex because they aggregate values across multiple batches of data and then produce one final value, so we need to introduce the concept of accumulators, and the physical representation of each aggregate expression needs to know how to produce an appropriate accumulator for the query engine to pass input data to.

Here are the main interfaces for representing aggregate expressions and accumulators.

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

The implementation for the `Max` aggregate expression would produce a specific MaxAccumulator.

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

Here is an example implementation of the MaxAccumulator.

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

## Physical Plans

With the physical expressions in place, we can now implement the physical plans for the various transformations that the query engine will support.

## Scan

The Scan execution plan simply delegates to a data source, passing in a projection to limit the number of columns to load into memory. No additional logic is performed.

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

## Projection

The projection execution plan simply evaluates the projection expressions against the input columns and then produces a record batch containing the derived columns. Note that for the case of projection expressions that reference existing columns by name, the derived column is simply a pointer or reference to the input column, so the underlying data values are not being copied.

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

## Selection (also known as Filter)

The selection execution plan is the first non-trivial plan, since it has conditional logic to determine which rows from the input record batch should be included in the output batches.

For each input batch, the filter expression is evaluated to return a bit vector containing bits representing the Boolean result of the expression, with one bit for each row. This bit vector is then used to filter the input columns to produce new output columns. This is a simple implementation that could be optimized for cases where the bit vector contains all ones or all zeros to avoid overhead of copying data to new vectors.

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

## Hash Aggregate

The HashAggregate plan is more complex than the previous plans because it must process all incoming batches and maintain a HashMap of accumulators and update the accumulators for each row being processed. Finally, the results from the accumulators are used to create one record batch at the end containing the results of the aggregate query.

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

## Joins

As the name suggests, the Join operator joins rows from two relations. There are a number of different types of joins with different semantics:

- `[INNER] JOIN`: This is the most commonly used join type and creates a new relation containing rows from both the left and right inputs. When the join expression consists only of equality comparisons between columns from the left and right inputs then the join is known as an "equi-join". An example of an equi-join would be `SELECT * FROM customer JOIN orders ON customer.id = order.customer_id`.
- `LEFT [OUTER] JOIN`: A left outer join produces rows that contain all values from the left input, and optionally rows from the right input. Where this is no match on the right-hand side then null values are produced for the right columns.
- `RIGHT [OUTER] JOIN`: This is the opposite of the left join. All rows from the right are returned along with rows from the left where available.
- `SEMI JOIN`: A semi join is similar to a left join but it only returns rows from the left input where there is match to the right input. No data is returned from the right input. Not all SQL implementations support semi joins explicitly and they are often written as subqueries instead. An example of a semi join would be `SELECT id FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`.
- `ANTI JOIN`: An into join is the opposite of a semi join. It only returns rows from the left input where this is match on the right input. An example of an anti join would be `SELECT id FROM foo WHERE NOT EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`.
- `CROSS JOIN`: A cross join returns every possible combination of rows from the left input combined with rows from the right input. If the left input contains 100 rows and the right input contains 200 rows then 20,000 rows will be returned. This is known as a cartesian product.

KQuery does not yet implement the join operator.

## Subqueries

Subqueries are queries within queries. They can be correlated or uncorrelated (involving a join to other relations or not). When a subquery returns a single value then it is known as a scalar subquery.

### Scalar subqueries

A scalar subquery returns a single value and can be used in many SQL expressions where a literal value could be used.

*Here is an example of a correlated scalar subquery:*

`SELECT id, name, (SELECT count(*) FROM orders WHERE customer_id = customer.id) AS num_orders FROM customers`

*Here is an example of an uncorrelated scalar subquery:*

`SELECT * FROM orders WHERE total > (SELECT avg(total) FROM sales WHERE customer_state = 'CA')`

Correlated subqueries are translated into joins before execution (this is explained in chapter 9).

Uncorrelated queries can be executed individually and the resulting value can be substituted into the top-level query.

### EXISTS and IN subqueries

The `EXISTS` and `IN` expressions (and their negated forms, `NOT EXISTS` and `NOT IN`) can be used to create semi-joins and anti-joins.

Here is an example of a semi-join that selects all rows from the left relation (`foo`) where there is a matching row returned by the subquery.

`SELECT id FROM foo WHERE EXISTS (SELECT * FROM bar WHERE foo.id = bar.id)`

Correlated subqueries are typically converted into joins during logical plan optimization (this is explained in chapter 9)

KQuery does not yet implement subqueries.

## Creating Physical Plans

With our physical plans in place, the next step is to build a query planner to create physical plans from logical plans, which we cover in the next chapter.

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
