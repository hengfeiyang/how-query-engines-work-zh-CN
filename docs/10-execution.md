# 查询执行

我们现在能够编写代码来对 CSV 文件执行优化的查询。

在使用 KQuery 执行查询之前，使用可信替代方案可能会很有用，这样我们就知道正确的结果应该是什么，并获取一些基线性能指标以供比较。

## Apache Spark 示例

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `spark` 模块中找到。_

首先，我们需要创建一个 Spark 上下文。请注意，我们使用单线程执行，以便我们可以与 KQuery 中单线程实现的性能进行相对公平的比较。

```scala
val spark = SparkSession.builder()
  .master("local[1]")
  .getOrCreate()
```

接下来，我们需要根据上下文将 CSV 文件注册为 DataFrame。

```scala
val schema = StructType(Seq(
  StructField("VendorID", DataTypes.IntegerType),
  StructField("tpep_pickup_datetime", DataTypes.TimestampType),
  StructField("tpep_dropoff_datetime", DataTypes.TimestampType),
  StructField("passenger_count", DataTypes.IntegerType),
  StructField("trip_distance", DataTypes.DoubleType),
  StructField("RatecodeID", DataTypes.IntegerType),
  StructField("store_and_fwd_flag", DataTypes.StringType),
  StructField("PULocationID", DataTypes.IntegerType),
  StructField("DOLocationID", DataTypes.IntegerType),
  StructField("payment_type", DataTypes.IntegerType),
  StructField("fare_amount", DataTypes.DoubleType),
  StructField("extra", DataTypes.DoubleType),
  StructField("mta_tax", DataTypes.DoubleType),
  StructField("tip_amount", DataTypes.DoubleType),
  StructField("tolls_amount", DataTypes.DoubleType),
  StructField("improvement_surcharge", DataTypes.DoubleType),
  StructField("total_amount", DataTypes.DoubleType)
))

val tripdata = spark.read.format("csv")
  .option("header", "true")
  .schema(schema)
  .load("/mnt/nyctaxi/csv/yellow_tripdata_2019-01.csv")

tripdata.createOrReplaceTempView("tripdata")
```

最后，我们可以开始针对 DataFrame 执行 SQL。

```scala
val start = System.currentTimeMillis()

val df = spark.sql(
  """SELECT passenger_count, MAX(fare_amount)
    |FROM tripdata
    |GROUP BY passenger_count""".stripMargin)

df.foreach(row => println(row))

val duration = System.currentTimeMillis() - start

println(s"Query took $duration ms")
```

在我的桌面电脑上执行此代码会生成以下输出。

```
[1,623259.86]
[6,262.5]
[3,350.0]
[5,760.0]
[9,92.0]
[4,500.0]
[8,87.0]
[7,78.0]
[2,492.5]
[0,36090.3]
Query took 14418 ms
```

## KQuery 示例

_本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `examples` 模块中找到。_

这是使用 KQuery 实现的等效查询。请注意，此代码与 Spark 示例不同，因为 KQuery 还没有指定 CSV 文件结构的选项，因此所有数据类型都是字符串，这意味着我们需要向查询计划添加显式转换以将 `fare_amount` 列转换为数字类型。

```kotlin
val time = measureTimeMillis {

val ctx = ExecutionContext()

val df = ctx.csv("/mnt/nyctaxi/csv/yellow_tripdata_2019-01.csv", 1*1024)
            .aggregate(
               listOf(col("passenger_count")),
               listOf(max(cast(col("fare_amount"), ArrowTypes.FloatType))))

val optimizedPlan = Optimizer().optimize(df.logicalPlan())
val results = ctx.execute(optimizedPlan)

results.forEach { println(it.toCSV()) }

println("Query took $time ms")
```

在我的桌面电脑上会生成以下输出。

```
Schema<passenger_count: Utf8, MAX: FloatingPoint(DOUBLE)>
1,623259.86
2,492.5
3,350.0
4,500.0
5,760.0
6,262.5
7,78.0
8,87.0
9,92.0
0,36090.3

Query took 6740 ms
```

我们可以看到结果与 Apache Spark 生成的结果相匹配。我们还发现，对于这种大小的输入，性能相当不错。Apache Spark 在处理更大的数据集时很可能会优于 KQuery，因为它针对“大数据”进行了优化。

## 删除查询优化器

让我们删除优化，看看它们对性能有多大帮助。

```kotlin
val time = measureTimeMillis {

val ctx = ExecutionContext()

val df = ctx.csv("/mnt/nyctaxi/csv/yellow_tripdata_2019-01.csv", 1*1024)
            .aggregate(
               listOf(col("passenger_count")),
               listOf(max(cast(col("fare_amount"), ArrowTypes.FloatType))))

val results = ctx.execute(df.logicalPlan())

results.forEach { println(it.toCSV()) }

println("Query took $time ms")
```

在我的桌面电脑上会生成以下输出。

```
1,623259.86
2,492.5
3,350.0
4,500.0
5,760.0
6,262.5
7,78.0
8,87.0
9,92.0
0,36090.3

Query took 36090 ms
```

结果是相同的，但查询的执行时间大约是原来的五倍。这清楚地显示了前一章讨论的映射下推优化的好处。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
