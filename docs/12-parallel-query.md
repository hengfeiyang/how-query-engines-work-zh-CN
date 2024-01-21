# Parallel Query Execution
So far, we have been using a single thread to execute queries against individual files. This approach is not very scalable, because queries will take longer to run with larger files or with multiple files. The next step is to implement distributed query execution so that query execution can utilize multiple CPU cores and multiple servers.

The simplest form of distributed query execution is parallel query execution utilizing multiple CPU cores on a single node using threads.

The NYC taxi data set is already conveniently partitioned because there is one CSV file for each month of each year, meaning that there are twelve partitions for the 2019 data set, for example. The most straightforward approach to parallel query execution would be to use one thread per partition to execute the same query in parallel and then combine the results. Suppose this code is running on a computer with six CPU cores with hyper-threading support. In that case, these twelve queries should execute in the same elapsed time as running one of the queries on a single thread, assuming that each month has a similar amount of data.

Here is an example of running an aggregate SQL query in parallel across twelve partitions. This example is implemented using Kotlin coroutines, rather than using threads directly.

The source code for this example can be found at `jvm/examples/src/main/kotlin/ParallelQuery.kt` in the KQuery GitHub repository.

Let us start with the single-threaded code for running one query against one partition.

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

With this in place, we can now write the following code to run this query in parallel across each of the twelve partitions of data.

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

Here is the output from this example, running on a desktop computer with *24 cores*.

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

As you can see, the total duration was around the same time as the slowest query.

Although we have successfully executed the aggregate query against the partitions, our result is a list of batches of data with duplicate values. For example, there will most likely be a result for `passenger_count=1` from each of the partitions.

## Combining Results

For simple queries consisting of projection and selection operators, the results of the parallel queries can be combined (similar to a SQL `UNION ALL` operation), and no further processing is required. More complex queries involving aggregates, sorts, or joins will require a secondary query to be run on the results of the parallel queries to combine the results. The terms "map" and "reduce" are often used to explain this two-step process. The "map" step refers to running one query in parallel across the partitions, and the "reduce" step refers to combining the results into a single result.

For this particular example, it is now necessary to run a secondary aggregation query almost identical to the aggregate query executed against the partitions. One difference is that the second query may need to apply different aggregate functions. For the aggregate functions `min`, `max`, and `sum`, the same operation is used in the map and reduce steps, to get the min of the min or the sum of the sums. For the count expression, we do not want the count of the counts. We want to see the sum of the counts instead.

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

This produces the final result set:

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

## Smarter Partitioning

Although the strategy of using one thread per file worked well in this example, it does not work as a general-purpose approach to partitioning. If a data source has thousands of small partitions, starting one thread per partition would be inefficient. A better approach would be for the query planner to decide how to share the available data between a specified number of worker threads (or executors).

Some file formats already have a natural partitioning scheme within them. For example, Apache Parquet files consist of multiple "row groups" containing batches of columnar data. A query planner could inspect the available Parquet files, build a list of row groups and then schedule reading these row groups across a fixed number of threads or executors.

It is even possible to apply this technique to unstructured files such as CSV files, but this is not trivial. It is easy to inspect the file size and break the file into equal-sized chunks, but a record could likely span two chunks, so it is necessary to read backward or forwards from a boundary to find the start or end of the record. It is insufficient to look for a newline character because these often appear within records and are also used to delimit records. It is common practice to convert CSV files into a structured format such as Parquet early on in a processing pipeline to improve the efficiency of subsequent processing.

## Partition Keys

One solution to this problem is to place files in directories and use directory names consisting of key-value pairs to specify the contents.

For example, we could organize the files as follows:

```
/mnt/nyxtaxi/csv/year=2019/month=1/tripdata.csv
/mnt/nyxtaxi/csv/year=2019/month=2/tripdata.csv
...
/mnt/nyxtaxi/csv/year=2019/month=12/tripdata.csv
```

Given this structure, the query planner could now implement a form of "predicate push down" to limit the number of partitions included in the physical query plan. This approach is often referred to as "partition pruning".

## Parallel Joins

When performing an inner join with a single thread, a simple approach is to load one side of the join into memory and then scan the other side, performing lookups against the data stored in memory. This classic Hash Join algorithm is efficient if one side of the join can fit into memory.

The parallel version of this is known as a Partitioned Hash Join or Parallel Hash Join. It involves partitioning both inputs based on the join keys and performing a classic Hash Join on each pair of input partitions.

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
