# Data Sources

_The source code discussed in this chapter can be found in the `datasource` module of the[ KQuery project](https://github.com/andygrove/how-query-engines-work)._

A query engine is of little use without a data source to read from and we want the ability to support multiple data sources, so it is important to create an interface that the query engine can use to interact with data sources. This also allows users to use our query engine with their custom data sources. Data sources are often files or databases but could also be in-memory objects.

## Data Source Interface

During query planning, it is important to understand the schema of the data source so that the query plan can be validated to make sure that referenced columns exist and that data types are compatible with the expressions being used to reference them. In some cases, the schema might not be available, because some data sources do not have a fixed schema and are generally referred to as "schema-less". JSON documents are one example of a schema-less data source.

During query execution, we need the ability to fetch data from the data source and need to be able to specify which columns to load into memory for efficiency. There is no sense loading columns into memory if the query doesn't reference them.

*KQuery DataSource Interface*

```kotlin
interface DataSource {

  /** Return the schema for the underlying data source */
  fun schema(): Schema

  /** Scan the data source, selecting the specified columns */
  fun scan(projection: List<String>): Sequence<RecordBatch>
}
```

## Data Source Examples

There are a number of data sources that are often encountered in data science or analytics.

### Comma-Separated Values (CSV)

CSV files are text files with one record per line and fields are separated with commas, hence the name "Comma Separated Values". CSV files do not contain schema information (other than optional column names on the first line in the file) although it is possible to derive the schema by reading the file first. This can be an expensive operation.

### JSON

The JavaScript Object Notation format (JSON) is another popular text-based file format. Unlike CSV files, JSON files are structured and can store complex nested data types.

### Parquet

Parquet was created to provide a compressed, efficient columnar data representation and is a popular file format in the Hadoop ecosystem. Parquet is built from the ground up with complex nested data structures in mind, and uses the [record shredding and assembly algorithm](https://github.com/julienledem/redelm/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper) described in the Dremel paper.

Parquet files contain schema information and data is stored in batches (referred to as "row groups") where each batch consists of columns. The row groups can contain compressed data and can also contain optional metadata such as minimum and maximum values for each column. Query engines can be optimised to use this metadata to determine when row groups can be skipped during a scan.

### Orc

The Optimized Row Columnar (Orc) format is similar to Parquet. Data is stored in columnar batches called "stripes".

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
