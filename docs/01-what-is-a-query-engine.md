# What Is a Query Engine?

A query engine is a piece of software that can execute queries against data to produce answers to questions, such as:

- What were my average sales by month so far this year?
- What were the five most popular web pages on my site in the past day?
- How does web traffic compare month-by-month with the previous year?

The most widespread query language is [Structured Query Language](https://en.wikipedia.org/wiki/SQL) (abbreviated as SQL). Many developers will have encountered relational databases at some point in their careers, such as MySQL, Postgres, Oracle, or SQL Server. All of these databases contain query engines that support SQL.

Here are some example SQL queries.

*SQL Example: Average Sales By Month*

```sql
SELECT month, AVG(sales)
FROM product_sales
WHERE year = 2020
GROUP BY month;
```

*SQL Example: Top 5 Web Pages Yesterday*

```sql
SELECT page_url, COUNT(*) AS num_visits
FROM apache_log
WHERE event_date = yesterday()
GROUP BY page_url
ORDER BY num_visits DESC
LIMIT 5;
```

SQL is powerful and widely understood but has limitations in the world of so-called "Big Data," where data scientists often need to mix in custom code with their queries. Platforms and tools such as Apache Hadoop, Apache Hive, and Apache Spark are now widely used to query and manipulate vast data volumes.

Here is an example that demonstrates how Apache Spark can be used to perform a simple aggregate query against a Parquet data set. The real power of Spark is that this query can be run on a laptop or on a cluster of hundreds of servers with no code changes required.

*Example of Apache Spark Query using DataFrame*

```scala
val spark: SparkSession = SparkSession.builder
  .appName("Example")
  .master("local[*]")
  .getOrCreate()

val df = spark.read.parquet("/mnt/nyctaxi/parquet")
  .groupBy("passenger_count")
  .sum("fare_amount")
  .orderBy("passenger_count")

df.show()
```

## Why Are Query Engines Popular?

Data is growing at an ever-increasing pace and often cannot fit on a single computer. Specialist engineering skills are needed to write distributed code for querying data, and it isn't practical to write custom code each time new answers are needed from data.

Query engines provide a set of standard operations and transformations that the end-user can combine in different ways through a simple query language or application programming interface and are tuned for good performance.

## What This Book Covers
This book provides an overview of every step involved in building a general-purpose query engine.

The query engine discussed in this book is a simple query engine developed specifically for this book, with the code being developed alongside writing the book content to make sure that I could write about topics while I was facing design decisions.

## Source Code

Full source code for the query engine discussed in this book is located in the following GitHub repository.

```
https://github.com/andygrove/how-query-engines-work
```

Refer to the README in the project for up-to-date instructions for building the project using Gradle.

## Why Kotlin?

The focus of this book is query engine design, which is generally programming language-agnostic. I chose Kotlin for this book because it is concise and easy to comprehend. It is also 100% compatible with Java, meaning that you can call Kotlin code from Java, and other Java-based languages, such as Scala.

However, the DataFusion query engine in the Apache Arrow project is also primarily based on the design in this book. Readers who are more interested in Rust than JVM can refer to the DataFusion source code in conjunction with this book.

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
