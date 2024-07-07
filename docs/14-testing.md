# 测试

查询引擎非常复杂，很容易无意中引入细微的错误，导致查询返回不正确的结果，因此进行严格的测试非常重要。

## 单元测试

第一步最好是为各个运算符和表达式编写单元测试，确保它们在给定输入下产生正确的输出。同时也要覆盖错误情况。

以下是编写单元测试时需要考虑的一些建议：

- 如果使用了意外的数据类型会发生什么？例如，在字符串输入上计算 `SUM`。
- 测试应涵盖边缘情况，例如对数值类型使用最小值和最大值，以及浮点类型中的 NaN（非数字），以确保它们被正确处理。
- 应该对下溢和上溢情况进行测试。例如，当两个长（64 位）整数类型相乘时会发生什么？
- 测试还应确保正确处理空值。

在编写这些测试时，能够构建具有任意数据的记录批次和列向量以用作运算符和表达式的输入非常重要。以下是此类实用方法的一个示例。

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

以下是一个针对包含两个双精度浮点值列的记录批进行“大于或等于”（`>=`）表达式的示例单元测试。 

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

## 集成测试

一旦单元测试到位，下一步就是编写集成测试，执行由多个运算符和表达式组成的查询，并断言它们产生预期的输出。

有几种流行的查询引擎集成测试方法：

- **命令式测试**: 硬编码查询和预期结果，可以作为代码编写或存储为包含查询和结果的文件。
- **比较性测试**: 这种方法涉及对另一个（可信赖的）查询引擎执行查询，并断言两个查询引擎产生相同的结果。
- **模糊测试**: 生成随机运算符和表达式树，以捕捉边缘情况并获得全面的测试覆盖。

## 模糊测试

查询引擎的复杂性很大程度上来自于运算符和表达式可以通过无限组合进行组合，因为运算符和表达式树具有嵌套性质，手工编写测试查询不太可能足够全面。

模糊测试是一种生成随机输入数据的技术。当应用于查询引擎时，这意味着创建随机查询计划。

以下是针对 DataFrame 创建随机表达式的示例。这是一种递归方法，可以生成深度嵌套的表达式树，因此构建最大深度机制非常重要。

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

以下是使用此方法生成的表达式示例。请注意，这里的列引用用哈希后的索引表示，例如 `#1` 代表索引为1的列。这个表达式几乎肯定是无效的（取决于查询引擎的实现），这是使用模糊测试时预期会发生的情况。这仍然有价值，因为它将测试手动编写测试时不会覆盖的错误条件。

```
#5 > 0.5459397414890019 < 0.3511239641785846 OR 0.9137719758607572 > -6938650321297559787 < #0 AND #3 < #4 AND 'qn0NN' OR '1gS46UuarGz2CdeYDJDEW3Go6ScMmRhA3NgPJWMpgZCcML1Ped8haRxOkM9F' >= -8765295514236902140 < 4303905842995563233 OR 'IAseGJesQMOI5OG4KrkitichlFduZGtjXoNkVQI0Alaf2ELUTTIci' = 0.857970478666058 >= 0.8618195163699196 <= '9jaFR2kDX88qrKCh2BSArLq517cR8u2' OR 0.28624225053564 <= 0.6363627130199404 > 0.19648131921514966 >= -567468767705106376 <= #0 AND 0.6582592932801918 = 'OtJ0ryPUeSJCcMnaLngBDBfIpJ9SbPb6hC5nWqeAP1rWbozfkPjcKdaelzc' >= #0 >= -2876541212976899342 = #4 >= -3694865812331663204 = 'gWkQLswcU' != #3 > 'XiXzKNrwrWnQmr3JYojCVuncW9YaeFc' >= 0.5123788261193981 >= #2
```

创建逻辑查询计划时可以采用类似的方法。

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

以下是此代码生成的一个逻辑查询计划示例。

```
Filter: 'VejBmVBpYp7gHxHIUB6UcGx' OR 0.7762591612853446
  Filter: 'vHGbOKKqR' <= 0.41876514212913307
    Filter: 0.9835090312561898 <= 3342229749483308391
      Filter: -5182478750208008322 < -8012833501302297790
        Filter: 0.3985688976088563 AND #1
          Filter: #5 OR 'WkaZ54spnoI4MBtFpQaQgk'
            Scan: employee.csv; projection=None
```

这种直接的模糊测试方法将产生高比例的无效计划。通过增加更多上下文意识，可以改进以减少创建无效逻辑计划和表达式的风险。例如，生成一个 `AND` 表达式时，可以生成左侧和右侧表达式来产生布尔结果。然而，仅仅创建正确的计划存在危险，因为这可能会限制测试覆盖范围。理想情况下，应该可以使用规则来配置模糊测试器，以生成具有不同特征的查询计划。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
