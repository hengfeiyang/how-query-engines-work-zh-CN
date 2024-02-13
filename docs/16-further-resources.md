# 更多资源

我希望你觉得这本书有用，并且您现在对查询引擎的内部结构有更好的了解。如果您认为某些主题没有被充分涵盖，或者根本没有涵盖，我很乐意听到你的想法，以便我考虑在未来的修订中添加额外的内容。

可以在 [Leanpub 网站](https://community.leanpub.com/t/feedback/2160) 上的公共论坛上发布反馈，或者通过 twitter 直接给我 [@andygrove_io](https://twitter.com/andygrove_io) 留言。

## 开源项目

有许多包含查询引擎的开源项目，使用这些项目可以更好的了解这个主题。以下只是流行的开源查询引擎的几个示例。

- Apache Arrow
- Apache Calcite
- Apache Drill
- Apache Hadoop
- Apache Hive
- Apache Impala
- Apache Spark
- Facebook Presto
- NVIDIA RAPIDS Accelerator for Apache Spark

## YouTube

我最近才发现 Andy Pavlo 的系列讲座，可以在 YouTube 上找到（[here](https://www.youtube.com/playlist?list=PLSE8ODhjZXjasmrEd2_Yi1deeE360zv5O)）。这不仅仅涵盖了查询引擎，还广泛地介绍了查询优化和执行方面的内容。强烈推荐观看这些视频。

## 样本数据

前面的章节引用了 [纽约市出租车及豪华轿车委员会行程记录数据](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) 数据集。黄色和绿色出租车行程记录包括上车和下车日期/时间、上车和下车地点、行程距离、明细票价、费率类型、付款类型和司机报告的乘客数量的字段。数据以 CSV 格式提供。 KQuery 项目包含用于将这些 CSV 文件转换为 Parquet 格式的源代码。

可以通过网站上的链接或直接从 S3 来下载数据。例如，Linux 或 Mac 上的用户可以通过以下命令使用 `curl` 或者 `wget` 下载 Yellow Taxis 2019 年 1 月的数据，还可以创建脚本以根据文件命名规范下载其他文件。

```
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv
```

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
