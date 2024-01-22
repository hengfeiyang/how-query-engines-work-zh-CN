# 什么是查询引擎？

查询引擎是一种软件，可以对数据执行查询以生成问题的答案，例如：

- 今年到目前为止，我的月平均销售额是多少？
- 过去一天我网站上最受欢迎的五个网页是什么？
- 与上一年相比，每月的网站流量如何？

最广泛使用的查询语言是[结构化查询语言](https://en.wikipedia.org/wiki/SQL)（简称SQL）。许多开发人员在其职业生涯的某个阶段都会遇到过关系数据库，例如 MySQL、Postgres、Oracle 或 SQL Server。所有这些数据库都包含支持 SQL 的查询引擎。

以下是一些 SQL 查询示例。

*SQL 示例：月平均销售额*

```sql
SELECT month, AVG(sales)
FROM product_sales
WHERE year = 2020
GROUP BY month;
```

*SQL 示例：昨天排名前 5 的网页*

```sql
SELECT page_url, COUNT(*) AS num_visits
FROM apache_log
WHERE event_date = yesterday()
GROUP BY page_url
ORDER BY num_visits DESC
LIMIT 5;
```

SQL 功能强大且被广泛理解，但在所谓的“大数据”世界中存在局限性，数据科学家通常需要将自定义代码与其查询混合在一起。现在，Apache Hadoop、Apache Hive 和 Apache Spark 等平台和工具现在广泛用于查询和操作海量数据。

以下示例演示了如何使用 Apache Spark 对 Parquet 数据集执行简单的聚合查询。Spark 的真正强大之处在于，该查询可以在笔记本电脑或数百台服务器的集群上运行，而无需更改代码。

*Apache Spark 使用 DataFrame 查询示例*

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

## 为什么查询引擎这么流行？

数据正以前所未有的速度增长，并且往往无法仅存储在单台计算机上。编写用于查询数据的分布式代码需要专业工程技能，并且每次需要从数据中获取新答案时都编写定制代码是不现实的。

查询引擎提供了一组标准操作和转换，最终用户可以通过简单的查询语言或应用程序编程接口以不同方式组合使用，并且调优以获得良好性能。

## 本书内容概述

本书提供了构建通用查询引擎所涉及的每个步骤的概览。

本书中讨论的查询引擎是专门为本书开发的简单查询引擎，代码是在编写本书内容的同时开发的，以确保我可以在面临设计决策时编写有关主题的内容。

## 源代码

本书中讨论的查询引擎的完整源代码位于以下 GitHub 存储库中。

```
https://github.com/andygrove/how-query-engines-work
```

请参阅项目中的README，获取使用Gradle构建项目的最新指南。

## 为什么使用 Kotlin？

本书的重点是查询引擎设计，这通常与编程语言无关。我之所以为这本书选择Kotlin，是因为它简洁易懂。它还与 Java 100% 兼容，这意味着您可以从 Java 和其他基于 Java 的语言（例如 Scala）调用 Kotlin 代码。

然而，Apache Arrow项目中的DataFusion查询引擎也主要基于本书的设计。对Rust比JVM更感兴趣的读者可以结合本书参考DataFusion源码。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
