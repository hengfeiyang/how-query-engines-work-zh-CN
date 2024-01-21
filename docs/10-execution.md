# Query Execution
We are now able to write code to execute optimized queries against CSV files.

Before we execute the query with KQuery, it might be useful to use a trusted alternative so that we know what the correct results should be and to get some baseline performance metrics for comparison.

## Apache Spark Example

_The source code discussed in this chapter can be found in the `spark` module of the[ KQuery project](https://github.com/andygrove/how-query-engines-work)._

First, we need to create a Spark context. Note that we are using a single thread for execution so that we can make a relatively fair comparison to the performance of the single threaded implementation in KQuery.

```scala
val spark = SparkSession.builder()
  .master("local[1]")
  .getOrCreate()
```

Next, we need to register the CSV file as a DataFrame against the context.

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

Finally, we can go ahead and execute SQL against the DataFrame.

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

Executing this code on my desktop produces the following output.

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

## KQuery Examples

_The source code discussed in this chapter can be found in the `examples` module of the[ KQuery project](https://github.com/andygrove/how-query-engines-work)._

Here is the equivalent query implemented with KQuery. Note that this code differs from the Spark example because KQuery doesn't have the option of specifying the schema of the CSV file yet, so all data types are strings, and this means that we need to add an explicit cast to the query plan to convert the `fare_amount` column to a numeric type.

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

This produces the following output on my desktop.

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

We can see that the results match those produced by Apache Spark. We also see that the performance is respectable for this size of input. It is very likely that Apache Spark will outperform KQuery with larger data sets since it is optimized for "Big Data".

## Removing The Query Optimizer

Let's remove the optimizations and see how much they helped with performance.

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

This produces the following output on my desktop.

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

The results are the same, but the query took about five times as long to execute. This clearly shows the benefit of the projection push-down optimization that was discussed in the previous chapter.

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
