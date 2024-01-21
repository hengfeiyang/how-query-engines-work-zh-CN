# Choosing a Type System

_The source code discussed in this chapter can be found in the `datatypes` module of the[ KQuery project](https://github.com/andygrove/how-query-engines-work)._

The first step in building a query engine is to choose a type system to represent the different types of data that the query engine will be processing. One option would be to invent a proprietary type system specific to the query engine. Another option is to use the type system of the data source that the query engine is designed to query from.

If the query engine is going to support querying multiple data sources, which is often the case, then there is likely some conversion required between each supported data source and the query engine's type system, and it will be important to use a type system capable of representing all the data types of all the supported data sources.

## Row-Based or Columnar?

An important consideration is whether the query engine will process data row-by-row or whether it will represent data in a columnar format.

Many of today's query engines are based on the [Volcano Query Planner](https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf) where each step in the physical plan is essentially an iterator over rows. This is a simple model to implement but tends to introduce per-row overheads that add up pretty quickly when running a query against billions of rows. This overhead can be reduced by instead iterating over batches of data. Furthermore, if these batches represent columnar data rather than rows, it is possible to use "vectorized processing" and take advantage of SIMD (Single Instruction Multiple Data) to process multiple values within a column with a single CPU instruction. This concept can be taken even further by leveraging GPUs to process much larger quantities of data in parallel.

## Interoperability

Another consideration is that we may want to make our query engine accessible from multiple programming languages. It is common for users of query engines to use languages such as Python, R, or Java. We may also want to build ODBC or JDBC drivers to make it easy to build integrations.

Given these requirements, it would be good to find an industry standard for representing columnar data and for exchanging this data efficiently between processes.

It will probably come as little surprise that I believe that Apache Arrow provides an ideal foundation.

## Type System

We will use Apache Arrow as the basis for our type system. The following Arrow classes are used to represent schema, fields, and data types.

- *Schema* provides metadata for a data source or the results from a query. A schema consists of one or more fields.
- *Field* provides the name and data type for a field within a schema, and specifies whether it allows null values or not.
- *FieldVector* provides columnar storage for data for a field.
- *ArrowType* represents a data type.

KQuery introduces some additional classes and helpers as an abstraction over the Apache Arrow type system.

KQuery provides constants that can be referenced for the supported Arrow data types

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

Rather than working directly with `FieldVector`, KQuery introduces a `ColumnVector` interface as an abstraction to provide more convenient accessor methods, avoiding the need to case to a specific `FieldVector` implementation for each data type.

```kotlin
interface ColumnVector {
  fun getType(): ArrowType
  fun getValue(i: Int) : Any?
  fun size(): Int
}
```

This abstraction also makes it possible to have an implementation for scalar values, avoiding the need to create and populate a `FieldVector` with a literal value repeated for every index in the column.

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

KQuery also provides a `RecordBatch` class to represent a batch of columnar data.

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

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
