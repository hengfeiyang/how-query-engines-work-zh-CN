# Further Resources

I hope that you found this book useful and that you now have a better understanding of the internals of query engines. If there are topics that you feel haven't been covered adequately, or at all, I would love to hear about it so I can consider adding additional content in a future revision of this book.

Feedback can be posted on the public forum on the [Leanpub site](https://community.leanpub.com/t/feedback/2160), or you can message me directly via twitter at [@andygrove_io](https://twitter.com/andygrove_io).

## Open-Source Projects

There are numerous open-source projects that contain query engines and working with these projects is a great way to learn more about the topic. Here are just a few examples of popular open-source query engines.

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

I only recently discovered Andy Pavlo's lecture series, which is available on YouTube ([here](https://www.youtube.com/playlist?list=PLSE8ODhjZXjasmrEd2_Yi1deeE360zv5O)). This covers much more than just query engines, but there is extensive content on query optimization and execution. I highly recommend watching these videos.

## Sample Data

Earlier chapters reference the [New York City Taxi & Limousine Commission Trip Record Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) data set. The yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts. The data is provided in CSV format. The KQuery project contains source code for converting these CSV files into Parquet format.

Data can be downloaded by following the links on the website or by downloading the files directly from S3. For example, users on Linux or Mac can use `curl` or `wget` to download the January 2019 data for Yellow Taxis with the following command and create scripts to download other files based on the file naming convention.

```
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv
```

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
