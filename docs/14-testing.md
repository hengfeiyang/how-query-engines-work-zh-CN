# 测试

Query engines are complex, and it is easy to inadvertently introduce subtle bugs that could result in queries returning incorrect results, so it is important to have rigorous testing in place.

## Unit Testing
An excellent first step is to write unit tests for the individual operators and expressions, asserting that they produce the correct output for a given input. It is also essential to cover error cases.

Here are some suggestions for things to consider when writing unit tests:

- What happens if an unexpected data type is used? For example, calculating `SUM` on an input of strings.
- Tests should cover edge cases, such as using the minimum and maximum values for numeric data types, and NaN (not a number) for floating point types, to ensure that they are handled correctly.
- Tests should exist for underflow and overflow cases. For example, what happens when two long (64-bit) integer types are multiplied?
- Tests should also ensure that null values are handled correctly.

When writing these tests, it is important to be able to construct record batches and column vectors with arbitrary data to use as inputs for operators and expressions. Here is an example of such a utility method.

```kotlin
private fun createRecordBatch(schema: Schema,
                              columns: List<List<Any?>>): RecordBatch {

    val rowCount = columns[0].size
    val root = VectorSchemaRoot.create(schema.toArrow(),
                                       RootAllocator(Long.MAX_VALUE))
    root.allocateNew()
    (0 until rowCount).forEach { row ->
        (0 until columns.size).forEach { col ->
            val v = root.getVector(col)
            val value = columns[col][row]
            when (v) {
                is Float4Vector -> v.set(row, value as Float)
                is Float8Vector -> v.set(row, value as Double)
                ...
            }
        }
    }
    root.rowCount = rowCount

    return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
}
```

Here is an example unit test for the "greater than or equals" (`>=`) expression being evaluated against a record batch containing two columns containing double-precision floating point values.

```kotlin
@Test
fun `gteq doubles`() {

    val schema = Schema(listOf(
            Field("a", ArrowTypes.DoubleType),
            Field("b", ArrowTypes.DoubleType)
    ))

    val a: List<Double> = listOf(0.0, 1.0,
                                 Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN)
    val b = a.reversed()

    val batch = createRecordBatch(schema, listOf(a,b))

    val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
    val result = expr.evaluate(batch)

    assertEquals(a.size, result.size())
    (0 until result.size()).forEach {
        assertEquals(if (a[it] >= b[it]) 1 else 0, result.getValue(it))
    }
}
```

## Integration Testing

Once unit tests are in place, the next step is to write integration tests that execute queries consisting of multiple operators and expressions and assert that they produce the expected output.

There are a few popular approaches to integration testing of query engines:

- **Imperative Testing**: Hard-coded queries and expected results, either written as code or stored as files containing the queries and results.
- **Comparative Testing**: This approach involves executing queries against another (trusted) query engine and asserting that both query engines produced the same results.
- **Fuzzing**: Generating random operator and expression trees to capture edge cases and get comprehensive test coverage.

## Fuzzing

Much of the complexity of query engines comes from the fact that operators and expressions can be combined through infinite combinations due to the nested nature of operator and expression trees, and it is unlikely that hand-coding test queries will be comprehensive enough.

Fuzzing is a technique for producing random input data. When applied to query engines, this means creating random query plans.

Here is an example of creating random expressions against a DataFrame. This is a recursive method and can produce deeply nested expression trees, so it is important to build in a maximum depth mechanism.

```kotlin
fun createExpression(input: DataFrame, depth: Int, maxDepth: Int): LogicalExpr {
    return if (depth == maxDepth) {
        // return a leaf node
        when (rand.nextInt(4)) {
            0 -> ColumnIndex(rand.nextInt(input.schema().fields.size))
            1 -> LiteralDouble(rand.nextDouble())
            2 -> LiteralLong(rand.nextLong())
            3 -> LiteralString(randomString(rand.nextInt(64)))
            else -> throw IllegalStateException()
        }
    } else {
        // binary expressions
        val l = createExpression(input, depth+1, maxDepth)
        val r = createExpression(input, depth+1, maxDepth)
        return when (rand.nextInt(8)) {
            0 -> Eq(l, r)
            1 -> Neq(l, r)
            2 -> Lt(l, r)
            3 -> LtEq(l, r)
            4 -> Gt(l, r)
            5 -> GtEq(l, r)
            6 -> And(l, r)
            7 -> Or(l, r)
            else -> throw IllegalStateException()
        }
    }
}
```

Here is example of an expression generated with this method. Note that column references are represented here with an index following a hash, e.g. `#1` represents column at index 1. This expression is almost certainly invalid (depending on the query engine implementation), and this is to be expected when using a fuzzer. This is still valuable because it will test error conditions that otherwise would not be covered when manually writing tests.

```
#5 > 0.5459397414890019 < 0.3511239641785846 OR 0.9137719758607572 > -6938650321297559787 < #0 AND #3 < #4 AND 'qn0NN' OR '1gS46UuarGz2CdeYDJDEW3Go6ScMmRhA3NgPJWMpgZCcML1Ped8haRxOkM9F' >= -8765295514236902140 < 4303905842995563233 OR 'IAseGJesQMOI5OG4KrkitichlFduZGtjXoNkVQI0Alaf2ELUTTIci' = 0.857970478666058 >= 0.8618195163699196 <= '9jaFR2kDX88qrKCh2BSArLq517cR8u2' OR 0.28624225053564 <= 0.6363627130199404 > 0.19648131921514966 >= -567468767705106376 <= #0 AND 0.6582592932801918 = 'OtJ0ryPUeSJCcMnaLngBDBfIpJ9SbPb6hC5nWqeAP1rWbozfkPjcKdaelzc' >= #0 >= -2876541212976899342 = #4 >= -3694865812331663204 = 'gWkQLswcU' != #3 > 'XiXzKNrwrWnQmr3JYojCVuncW9YaeFc' >= 0.5123788261193981 >= #2
```

A similar approach can be taken when creating logical query plans.

```kotlin
fun createPlan(input: DataFrame,
               depth: Int,
               maxDepth: Int,
               maxExprDepth: Int): DataFrame {

    return if (depth == maxDepth) {
        input
    } else {
        // recursively create an input plan
        val child = createPlan(input, depth+1, maxDepth, maxExprDepth)
        // apply a transformation to the plan
        when (rand.nextInt(2)) {
            0 -> {
                val exprCount = 1.rangeTo(rand.nextInt(1, 5))
                child.project(exprCount.map {
                    createExpression(child, 0, maxExprDepth)
                })
            }
            1 -> child.filter(createExpression(input, 0, maxExprDepth))
            else -> throw IllegalStateException()
        }
    }
}
```

Here is an example of a logical query plan produced by this code.

```
Filter: 'VejBmVBpYp7gHxHIUB6UcGx' OR 0.7762591612853446
  Filter: 'vHGbOKKqR' <= 0.41876514212913307
    Filter: 0.9835090312561898 <= 3342229749483308391
      Filter: -5182478750208008322 < -8012833501302297790
        Filter: 0.3985688976088563 AND #1
          Filter: #5 OR 'WkaZ54spnoI4MBtFpQaQgk'
            Scan: employee.csv; projection=None
```

This straightforward approach to fuzzing will produce a high percentage of invalid plans. It could be improved to reduce the risk of creating invalid logical plans and expressions by adding more contextual awareness. For example, generating an `AND` expression could generate left and right expressions that produce a Boolean result. However, there is a danger in only creating correct plans because it could limit the test coverage. Ideally, it should be possible to configure the fuzzer with rules for producing query plans with different characteristics.

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
