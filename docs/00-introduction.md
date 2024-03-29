# 简介

自从开始我的第一份软件工程工作以来，我就对数据库和查询语言着迷。向计算机提出问题并有效地获取有意义的数据几乎像魔法一样。在多年作为通用软件开发者和数据技术终端用户的经验后，我开始在一家初创公司工作，这让我深入到了分布式数据库开发的领域。当我开始这段旅程时，我多么希望这本书已经存在。虽然这只是一本入门级的书籍，但我希望能够揭开查询引擎如何工作的神秘面纱。

我对查询引擎的兴趣最终使我参与了 [Apache Arrow](https://arrow.apache.org/) 项目，我在 2018 年捐赠了最初的 Rust 实现，然后在 2019 年捐赠了 [DataFusion](https://github.com/apache/arrow-datafusion) 内存查询引擎，最后在 2021 年捐赠了 [Ballista](https://github.com/apache/arrow-ballista) 分布式计算项目. 我不打算再构建 Arrow 项目之外的任何其他东西，并且现在我将继续为 Arrow 中的这些项目做出贡献。

Arrow 项目现在有许多活跃的提交者和贡献者致力于 Rust 实现，与我最初的贡献相比，已经有了显著的改进。

尽管 Rust 是高性能查询引擎的绝佳选择，但它并不适合教授查询引擎的相关概念，因此我在编写本书时使用 Kotlin 构建了一个新的查询引擎。Kotlin 是一种非常简洁且易于阅读的语言，使得可以在本书中包含源代码示例。我鼓励您在阅读本书时熟悉源代码，并考虑做出一些贡献。没有比动手实践更好的学习方法了！

本书中介绍的查询引擎最初打算作为 [Ballista](https://github.com/apache/arrow-ballista) 项目（并曾经是）的一部分，但随着项目的发展，显然将查询引擎保留在 Rust 中并通过 UDF 机制支持 Java 和其他语言，而不是在多种语言中复制大量的查询执行逻辑更有意义。

现在 [Ballista](https://github.com/apache/arrow-ballista) 已捐赠给 [Apache Arrow](https://arrow.apache.org/)，我已经更新了本书，将配套存储库中的查询引擎简称为“KQuery”，是 Kotlin 查询引擎的缩写，但如果有人有更好的名称建议，请告诉我！

本书内容更新将免费提供，请偶尔回来查看或者关注我的 Twitter (@andygrove_io) 以便在有新内容是收到通知。

## 反馈

如果您对这本书有任何反馈，请通过 Twitter @andygrove_io 向我发送私信或发送电子邮件至 agrove@apache.org。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
