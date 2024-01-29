# Apache Arrow

Apache Arrow 最初是作为列式数据内存格式的规范而开始的，并以 Java 和 C++ 实现。这种内存格式对于在支持 SIMD（单指令，多数据）的现代硬件如 CPU 和 GPU 上进行向量化处理非常高效。

采用标准化的内存数据格式有几个好处：

- Python 或 Java 等高级语言可以通过传递指向数据的指针而不是以不同的格式复制数据（这将非常贵）来调用 Rust 或 C++ 等低级语言来执行计算密集型任务。
- 数据可以在进程之间有效地传输，而无需大量序列化开销，因为内存格式也是网络格式（尽管数据也可以被压缩）。
- 它应该使在数据科学和数据分析领域的各种开源和商业项目之间构建连接器、驱动程序和集成变得更加容易，并允许开发人员使用他们最喜欢的语言来利用这些平台。

Apache Arrow 现在有多种编程语言的实现，包括 C、C++、C#、Go、Java、JavaScript、Julia、MATLAB、Python、R、Ruby 和 Rust。

## Arrow 内存模型

[Arrow 网站](https://arrow.apache.org/docs/format/Columnar.html) 上详细描述了内存模型，但本质上每一列都由保存原始数据的单个向量表示，以及表示空值和可变宽度类型的原始数据偏移量的单独向量。

## 进程间通信（IPC）

正如我之前提到的，可以通过传递指向数据的指针来在进程之间传递数据。然而，接收进程需要知道如何解释这些数据，因此定义了 IPC 格式来交换元数据，例如结构信息。Arrow 使用 Google Flatbuffers 来定义元数据格式。

## 计算核心

Apache Arrow 的范围已经扩展到为评估表达式提供计算库。Java、C++、C、Python、Ruby、Go、Rust 和 JavaScript 实现包含了用于在 Arrow 内存上执行计算的计算库。

由于本书主要涉及 Java 实现，值得一提的是 [Dremio](https://www.dremio.com/) 最近捐赠了 [Gandiva](https://github.com/apache/arrow/tree/main/cpp/src/gandiva)，这是一个将表达式编译为 LLVM 并支持 SIMD 的 Java 库。JVM 开发人员可以将操作委托给 [Gandiva](https://github.com/apache/arrow/tree/main/cpp/src/gandiva) 库，并从 Java 本身无法实现的性能提升中获益。

## Arrow Flight 协议

最近，Arrow 定义了一种用于高效地在网络上流式传输 Arrow 数据的 [Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) 协议，Flight 协议基于 [gRPC](https://grpc.io/) 和 [Google Protocol Buffers](https://protobuf.dev/)。

Flight 协议定义了一个 FlightService，具有以下方法：

### Handshake

客户端和服务端之间的握手。根据服务端的不同，可能需要握手来确定应用于未来操作的 Token。根据验证机制请求和响应都是允许多次往返的数据流。

### ListFlights

给定特定条件获取可用流列表。大多数 flight 服务都会公开一个或多个可供检索的流。此 API 允许列出可供使用的流。用户还可以提供条件，这些条件可以限制可以通过此接口列出的流的子集。每个 flight 服务都允许自己定义如何使用的条件。

### GetFlightInfo

对于给定的 FlightDescriptor，获取有关如何使用 flight 的信息。如果接口的消费者已经可以识别要使用的特定 flight，那么这是一个有用的接口。该接口还可以允许消费者通过指定的 descriptor 生成 flight 流。例如，一个 flight descriptor 可能包含将执行的 SQL 语句或 [Pickled Python](https://docs.python.org/3/library/pickle.html) 操作。在这些情况下，之前在 ListFilghts 可用流列表中未提供的流，反而在特定的 Flight 服务定义的期间可用。

### GetSchema

对于给定的 FlightDescriptor，获取在 Schema.fbs::Schema 中描述的结构。当消费者需要 flight 流的结构时使用此功能。与 GetFlightInfo 类似，此接口可能会生成一个以前在 ListFlights 中不可用的新 flight。

### DoGet

检索与引用 ticket 相关的特定描述符所关联的单个流。一个 Flight 可以由一个或多个流组成，其中每个流都可以使用一个独立的 Flight 服务用来管理数据流集合的 opaque ticket 进行检索。

### DoPut

将流推送到与特定 flight 流关联的 flight 服务。这允许 flight 服务的客户端上传数据流。根据特定的 flight 服务，可以允许客户端消费者上传每个描述符的单个流或无限数量的流。后者，服务可能会实现 “seal” 操作，一旦所有流都上传完毕，该操作就可以应用于描述符。

### DoExchange

为给定描述符打开双向数据通道。这允许客户端在单个逻辑流中发送和接收任意 Arrow 数据和应用程序的特定元数据。与 DoGet/DoPut 相比，这更适合客户端将计算（而不是存储）卸载到 Flight 服务。

### DoAction

除了可能可用的 ListFlights、GetFlightInfo、DoGet、DoPut 操作之外， flight 服务还可以支持任意数量的简单操作。DoAction 允许 flight 客户端针对 flight 服务执行特定操作。一个操作包括特定于正在执行的操作类型的不透明请求和响应对象。

### ListActions

一个 flight 服务公开其所有可用操作类型及其描述。这使得不同的 flight 消费者能够了解 flight 服务的功能。

## Arrow Flight SQL

有人提议向 Arrow Flight 添加 SQL 功能。在撰写本文时（2021年1月），已经有一个针对 C++ 实现的 PR，跟踪问题为 [ARROW-14698](https://issues.apache.org/jira/browse/ARROW-14698)。

## 查询引擎

### DataFusion

Arrow 的 Rust 实现包含一个名为 DataFusion 的内存查询引擎，该引擎于 2019 年捐赠给该项目。该项目正在迅速成熟，并越来越受到关注。例如，InfluxData 正在利用 DataFusion 构建下一代 InfluxDB 的核心。

### Ballista

Ballista 是一个分布式计算平台，主要用 Rust 实现，并由 Apache Arrow 提供支持。它构建了一种架构允许其他编程语言（例如 Python、C++ 和 Java）作为一等公民而无需额外序列化成本。

Ballista 中的基础技术包括：

- **Apache Arrow** 用于内存模型和类型系统。
- **Apache Arrow Flight** 协议用于进程间高效数据传输。
- **Apache Arrow Flight SQL** 协议供商业智能工具和JDBC驱动程序连接到 Ballista 集群。
- **Google Protocol Buffers**  用于序列化查询计划。
- **Docker** 用于打包执行器以及用户定义代码。
- **Kubernetes** 用于部署和管理执行器 docker 容器。

Ballista 于 2021 年捐赠给 Arrow 项目，虽然能够以良好的性能运行流行的 TPC-H 基准测试中的大量查询，但尚未准备好投入生产使用。

### C++ 查询引擎

C++ 实现正在进行添加查询引擎的工作，当前的重点是实现高效的计算原语和数据集 API。

*这本书还可通过 [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work) 购买 ePub、MOBI 和 PDF格式版本。*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
