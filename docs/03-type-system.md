# 选择一个类型系统

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `datatypes` 模块中找到。_

构建查询引擎的第一步是选择一个类型系统来表示查询引擎将要处理的不同类型的数据。一种选择是发明一种专门针对查询引擎的专有类型系统。另一种选择是使用查询引擎索要查询的数据源的类型系统。

如果查询引擎要支持查询多个数据源（通常是这种情况），那么每个支持的数据源和查询引擎的类型系统之间可能需要进行一些转换，因此使用一个能够代表所有支持的数据源的所有数据类型的类型系统将非常重要。

## 基于行还是基于列？

一个重要的考虑因素是查询引擎将逐行处理数据还是以列式格式表示数据。

如今许多的查询引擎都是基于 [Volcano Query Planner](https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf)，其在物理计划中每个步骤本质上都是对行进行迭代。这种模型易于实现，但在对数十亿行运行查询时往往会导致每行开销迅速增加。通过对数据进行批量迭代，可以减少这种开销。此外，如果这些批量数据以列的方式而不是行的方式表示数据，那么就可以使用 “向量化处理” 并利用 SIMD (单指令多数据) 的优势，用单条 CPU 指令处理一列中的多个值。这个概念还可以更进一步，利用 GPU 来并行处理更大量的数据。

## 互操作性

另一个考虑因素是我们可能希望使我们的查询引擎可以通过多种编程语言访问。查询引擎的用户通常使用 Python、R 或 Java 等语言。我们可能还希望构建 ODBC 或 JDBC 驱动程序，以便轻松构建集成。

考虑到这些需求，最好找到一个行业标准来表示列式数据并高效地在进程之间交换这些数据。

我相信 Apache Arrow 提供了理想的基础，这一点可能并不奇怪。

## 类型系统

我们将使用 Apache Arrow 作为我们类型系统的基础。以下 Arrow 类用于表示结构、字段和数据类型。

- *Schema* 为数据源或查询结果提供元数据，结构由一个或多个字段组成。
- *Field* 为结构中字段的名称和数据类型，并指定它是否允许空值。
- *FieldVector* 为字段数据提供列式存储。
- *ArrowType* 表示一种数据类型。

KQuery 引入了一些额外的类和助手作为对 Apache Arrow 类型系统的抽象。

KQuery 为受支持的 Arrow 数据类型提供了可引用的常量。

```kotlin
object ArrowTypes {
    val BooleanType = ArrowType.Bool()
    val Int8Type = ArrowType.Int(8, true)
    val Int16Type = ArrowType.Int(16, true)
    val Int32Type = ArrowType.Int(32, true)
    val Int64Type = ArrowType.Int(64, true)
    val UInt8Type = ArrowType.Int(8, false)
    val UInt16Type = ArrowType.Int(16, false)
    val UInt32Type = ArrowType.Int(32, false)
    val UInt64Type = ArrowType.Int(64, false)
    val FloatType = ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    val DoubleType = ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    val StringType = ArrowType.Utf8()
}
```

KQuery 并没有直接使用 `FieldVector`，而是引入了一个 `ColumnVector` 接口作为抽象，以提供更方便的访问方法，从而避免了为每种数据类型都使用特定的 `FieldVector` 实现。

```kotlin
interface ColumnVector {
  fun getType(): ArrowType
  fun getValue(i: Int) : Any?
  fun size(): Int
}
```

这种抽象也使得标量值的实现成为可能，从而避免了用字面值为列中每个索引重复创建和填入 `FieldVector`。

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

KQuery 还提供了一个 `RecordBatch` 类来表示一批列式数据。


```kotlin
class RecordBatch(val schema: Schema, val fields: List<ColumnVector>) {

  fun rowCount() = fields.first().size()

  fun columnCount() = fields.size

  /** Access one column by index */
  fun field(i: Int): ColumnVector {
      return fields[i]
  }

}
```

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
