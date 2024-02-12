# 数据源

> 本章所讨论的源代码可以在 [KQuery 项目](https://github.com/andygrove/how-query-engines-work) 的 `datasource` 模块中找到。

查询引擎如果没有数据源可读取，那么它几乎无用武之地。我们希望能够支持多个数据源，因此创建一个接口让查询引擎能与数据源交互非常重要。这还允许用户将我们的查询引擎与他们的自定义数据源配合使用。数据源通常是文件或数据库，但也可以是内存中的对象。

## 数据源接口

在查询计划阶段，了解数据源的结构非常重要，以便可以验证查询计划以确保引用的列存在并且数据类型与用于引用它们的表达式兼容。在某些情况下，结构可能不可用，因为某些数据源没有固定结构，通常称为“无结构（schema-less）”。 JSON 文档便是一种无结构数据源的例子。

在查询执行阶段，我们需要能够从数据源获取数据，并且需要能够指定将哪些列加载到内存中以提高效率。如果查询不涉及这些列，则没必要将其加载到内存中。

*KQuery 数据源接口*

```kotlin
interface DataSource {

  /** Return the schema for the underlying data source */
  fun schema(): Schema

  /** Scan the data source, selecting the specified columns */
  fun scan(projection: List<String>): Sequence<RecordBatch>
}
```

## 数据源示例

在数据科学或分析领域经常遇到许多数据源。

### 逗号分隔值 (CSV)

CSV 文件是文本文件，每行一条记录，字段之间用逗号分隔，因此称为“逗号分隔值”。 CSV 文件不包含结构信息（文件第一行的可选列名称除外），尽管可以通过先读取文件来推导出结构，这可能是一项昂贵的操作。

### JSON

JavaScript 对象表示法格式 (JSON) 是另一种流行的基于文本的文件格式。与 CSV 文件不同，JSON 文件是结构化的并且可以存储复杂的嵌套数据类型。

### Parquet

Parquet 旨在提供压缩、高效的列式数据表示，是 Hadoop 生态系统中流行的文件格式。Parquet 从一开始就考虑到了复杂的嵌套数据结构，并使用 Dremel 论文中描述的[record shredding and assembly algorithm](https://github.com/julienledem/redelm/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper) 算法构建。

Parquet 文件包含结构信息和批量存储的数据（称为“row groups”），其中每个批次若干列组成。`row groups` 可以包含压缩数据，还可以包含可选元数据，例如每列的最小值和最大值。查询引擎以使用此元数据来确定在扫描期间何时可以跳过某些 `row groups`。

### Orc

The Optimized Row Columnar (Orc) 优化行列 格式类似于 Parquet，数据以称为“stripes”的列式批量格式存储。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
