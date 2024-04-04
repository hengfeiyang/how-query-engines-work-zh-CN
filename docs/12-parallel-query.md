# 执行并发查询

到目前为止，我们一直使用单个线程来对单个文件执行查询。这种方法的可扩展性不是很强，因为对于较大的文件或多个文件，查询将需要更长的时间来运行。下一步是实现分布式查询执行，以便查询执行可以利用多个CPU核心和多个服务器。

分布式查询执行的最简单形式是使用线程在单个节点上利用多个 CPU 核心并行查询执行。

纽约市出租车数据集已经方便地进行分区，因为每年每个月都有一个 CSV 文件，这意味着例如 2019 年数据集有 12 个分区。并行查询执行的最直接方法是每个分区使用一个线程并行执行相同的查询，然后合并结果。假设此代码运行在有六个 CPU 核心且支持超线程的计算机上。在这种情况下，假设每个月都有相似的数据量，那么这 12 个查询的执行时间应该与在单个线程上运行其中一个查询的时间相同。

以下是在 12 个分区并行运行聚合 SQL 查询的示例。本示例是使用 Kotlin 协程实现的，而不是直接使用线程。

此示例的源代码可以在 KQuery GitHub 仓库的 `jvm/examples/src/main/kotlin/ParallelQuery.kt` 中找到。

让我们从针对一个分区运行一个查询的单线程代码开始。

```kotlin
fun executeQuery(path: String, month: Int, sql: String): List<RecordBatch> {
  val monthStr = String.format("%02d", month);
  val filename = "$path/yellow_tripdata_2019-$monthStr.csv"
  val ctx = ExecutionContext()
  ctx.registerCsv("tripdata", filename)
  val df = ctx.sql(sql)
  return ctx.execute(df).toList()
}
```

有了这个，我们现在可以编写以下代码来并行运行 12 个分区数据的查询。

```kotlin
val start = System.currentTimeMillis()
val deferred = (1..12).map {month ->
  GlobalScope.async {

    val sql = "SELECT passenger_count, " +
        "MAX(CAST(fare_amount AS double)) AS max_fare " +
        "FROM tripdata " +
        "GROUP BY passenger_count"

    val start = System.currentTimeMillis()
    val result = executeQuery(path, month, sql)
    val duration = System.currentTimeMillis() - start
    println("Query against month $month took $duration ms")
    result
  }
}
val results: List<RecordBatch> = runBlocking {
  deferred.flatMap { it.await() }
}
val duration = System.currentTimeMillis() - start
println("Collected ${results.size} batches in $duration ms")
```

这是在一台具有 *24核心* 的桌面电脑上运行此示例的输出结果。

```
Query against month 8 took 17074 ms
Query against month 9 took 18976 ms
Query against month 7 took 20010 ms
Query against month 2 took 21417 ms
Query against month 11 took 21521 ms
Query against month 12 took 22082 ms
Query against month 6 took 23669 ms
Query against month 1 took 23735 ms
Query against month 10 took 23739 ms
Query against month 3 took 24048 ms
Query against month 5 took 24103 ms
Query against month 4 took 25439 ms
Collected 12 batches in 25505 ms
```

如你所见，总持续时间与最慢查询的时间大致相同。

尽管我们已经成功地对分区执行了聚合查询，但我们的结果是具有重复值的批次数据的列表。例如，每个分区很可能都会产生`passenger_count=1` 的结果。

## 合并结果

对于由映射和选择运算符组成的简单查询，可以组合并行查询的结果（类似于SQL `UNION ALL` 操作），并且不需要进一步处理。涉及聚合、排序或联表的更复杂的查询将需要对并行查询的结果运行辅助查询以合并结果。术语 “map” 和 “reduce” 经常用来解释这个两步过程。 “map” 步骤是指跨分区并行运行一个查询，“reduce” 步骤是指将结果合并为单个结果。

对于这个特定的示例，现在需要运行与针对分区执行的聚合查询几乎相同的辅助聚合查询。一个区别是第二个查询可能需要应用不同的聚合函数。对于聚合函数 `min`、`max` 和 `sum` 在映射和归约步骤中使用相同的操作，以获得最小值的最小值或总和的总和。对于计数表达式，我们不需要计数结果的数量。我们希望看到所有计数的总和。

```kotlin
val sql = "SELECT passenger_count, " +
        "MAX(max_fare) " +
        "FROM tripdata " +
        "GROUP BY passenger_count"

val ctx = ExecutionContext()
ctx.registerDataSource("tripdata", InMemoryDataSource(results.first().schema, results))
val df = ctx.sql(sql)
ctx.execute(df).forEach { println(it) }
```

这会产生最终结果集： 

```
1,671123.14
2,1196.35
3,350.0
4,500.0
5,760.0
6,262.5
7,80.52
8,89.0
9,97.5
0,90000.0
```

## 更智能的分区

尽管每个文件使用一个线程的策略在此示例中效果很好，但它不能作为一种通用的分区方法。如果一个数据源有数千个小分区，那么每个分区启动一个线程将不会高效。更好的方法是让查询规划器决定如何在指定数量的工作线程（或执行器）之间共享可用数据。

某些文件格式已经天然具有分区方案。例如，Apache Parquet 文件由多个包含批量列式数据的 “行组” 组成。查询规划器可以检查可用的 Parquet 文件，构建行组列表，然后安排固定数量的线程或执行器读取这些行组。

甚至可以将此技术应用于非结构化文件（例如 CSV 文件），但这并非易事。检查文件大小并将文件分成大小相等的块很容易，但一条记录可能跨越两个块，因此有必要从边界向后或向前读取以找到记录的开头或结尾。查找换行符是不够的，因为它们经常出现在记录中，并且也用于分隔记录。通常的做法是在处理管道中尽早将 CSV 文件转换为结构化格式（例如 Parquet），以提高后续处理的效率。

## 分区键

解决这个问题的一种方法是将文件放在目录中，并使用由键值对组成的目录名称来指定内容。

例如，我们可以按如下方式组织文件：

```
/mnt/nyxtaxi/csv/year=2019/month=1/tripdata.csv
/mnt/nyxtaxi/csv/year=2019/month=2/tripdata.csv
...
/mnt/nyxtaxi/csv/year=2019/month=12/tripdata.csv
```

有了这个结构，查询计划程序现在可以实现一种 “谓词下推（predicate push down）” 形式，以限制物理查询计划中包含的分区数量。这种方法通常称为“分区修剪（partition pruning）”。

## 并发联表

当用单线程执行内连接时，一个简单的方法是将连接的一侧加载到内存中，然后扫描另一侧，在内存中存储的数据上进行查找。如果连接的一方能够装入内存，则此经典哈希连接算法非常高效。

其并行版本称为分区哈希连接或并行哈希连接。它涉及根据连接键对两个输入进行分区，并对每对输入分区执行经典的哈希连接。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
