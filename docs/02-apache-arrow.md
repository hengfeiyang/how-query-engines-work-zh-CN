# Apache Arrow

Apache Arrow started as a specification for a memory format for columnar data, with implementations in Java and C++. The memory format is efficient for vectorized processing on modern hardware such as CPUs with SIMD (Single Instruction, Multiple Data) support and GPUs.

There are several benefits to having a standardized memory format for data:

- High-level languages such as Python or Java can make calls into lower-level languages such as Rust or C++ for compute-intensive tasks by passing pointers to the data, rather than making a copy of the data in a different format, which would be very expensive.
- Data can be transferred between processes efficiently without much serialization overhead because the memory format is also the network format (although data can also be compressed).
- It should make it easier to build connectors, drivers, and integrations between various open-source and commercial projects in the data science and data analytics space and allow developers to use their favorite language to leverage these platforms.

Apache Arrow now has implementations in many programming languages, including C, C++, C#, Go, Java, JavaScript, Julia, MATLAB, Python, R, Ruby, and Rust.

## Arrow Memory Model

The memory model is described in detail on the [Arrow web site](https://arrow.apache.org/docs/format/Columnar.html), but essentially each column is represented by a single vector holding the raw data, along with separate vectors representing null values and offsets into the raw data for variable-width types.

## Inter-Process Communication (IPC)

As I mentioned earlier, data can be passed between processes by passing a pointer to the data. However, the receiving process needs to know how to interpret this data, so an IPC format is defined for exchanging metadata such as schema information. Arrow uses Google Flatbuffers to define the metadata format.

## Compute Kernels

The scope of Apache Arrow has expanded to provide computational libraries for evaluating expressions against data. The Java, C++, C, Python, Ruby, Go, Rust, and JavaScript implementations contain computational libraries for performing computations on Arrow memory.

Since this book mostly refers to the Java implementation, it is worth pointing out that Dremio recently donated Gandiva, which is a Java library that compiles expressions down to LLVM and supports SIMD. JVM developers can delegate operations to the Gandiva library and benefit from performance gains that wouldn't be possible natively in Java.

## Arrow Flight Protocol

More recently, Arrow has defined a [Flight protocol](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) for efficiently streaming Arrow data over the network. Flight is based on gRPC and Google Protocol Buffers.

The Flight protocol defines a FlightService with the following methods:

### Handshake

Handshake between client and server. Depending on the server, the handshake may be required to determine the token that should be used for future operations. Both request and response are streams to allow multiple round-trips depending on the auth mechanism.

### ListFlights

Get a list of available streams given a particular criteria. Most flight services will expose one or more streams that are readily available for retrieval. This API allows listing the streams available for consumption. A user can also provide a criteria. The criteria can limit the subset of streams that can be listed via this interface. Each flight service allows its own definition of how to consume criteria.

### GetFlightInfo

For a given FlightDescriptor, get information about how the flight can be consumed. This is a useful interface if the consumer of the interface can already identify the specific flight to consume. This interface can also allow a consumer to generate a flight stream through a specified descriptor. For example, a flight descriptor might be something that includes a SQL statement or a Pickled Python operation that will be executed. In those cases, the descriptor will not be previously available within the list of available streams provided by ListFlights, but will be available for consumption for the duration defined by the specific flight service.

### GetSchema

For a given FlightDescriptor, get the Schema as described in Schema.fbs::Schema. This is used when a consumer needs the Schema of flight stream. Similar to GetFlightInfo, this interface may generate a new flight that was not previously available in ListFlights.

### DoGet

Retrieve a single stream associated with a particular descriptor associated with the referenced ticket. A Flight can be composed of one or more streams where each stream can be retrieved using a separate opaque ticket that the flight service uses for managing a collection of streams.

### DoPut

Push a stream to the flight service associated with a particular flight stream. This allows a client of a flight service to upload a stream of data. Depending on the particular flight service, a client consumer could be allowed to upload a single stream per descriptor or an unlimited number. In the latter, the service might implement a 'seal' action that can be applied to a descriptor once all streams are uploaded.

### DoExchange

Open a bidirectional data channel for a given descriptor. This allows clients to send and receive arbitrary Arrow data and application-specific metadata in a single logical stream. In contrast to DoGet/DoPut, this is more suited for clients offloading computation (rather than storage) to a Flight service.

### DoAction
Flight services can support an arbitrary number of simple actions in addition to the possible ListFlights, GetFlightInfo, DoGet, DoPut operations that are potentially available. DoAction allows a flight client to do a specific action against a flight service. An action includes opaque request and response objects that are specific to the type of action being undertaken.

### ListActions
A flight service exposes all of the available action types that it has along with descriptions. This allows different flight consumers to understand the capabilities of the flight service.

## Arrow Flight SQL
There is a proposal to add SQL capabilities to Arrow Flight. At the time of writing (Jan 2021), there is a PR up for a C++ implementation and the tracking issue is [ARROW-14698](https://issues.apache.org/jira/browse/ARROW-14698).

## Query Engines

### DataFusion

The Rust implementation of Arrow contains an in-memory query engine named DataFusion, which was donated to the project in 2019. This project is maturing rapidly and is gaining traction. For example, InfluxData is building the core of the next generation of InfluxDB by leveraging DataFusion.

### Ballista

Ballista is a distributed compute platform primarily implemented in Rust, and powered by Apache Arrow. It is built on an architecture that allows other programming languages (such as Python, C++, and Java) to be supported as first-class citizens without paying a penalty for serialization costs.

The foundational technologies in Ballista are:

- **Apache Arrow** for the memory model and type system.
- **Apache Arrow Flight** protocol for efficient data transfer between processes.
- **Apache Arrow Flight SQL** protocol for use by business intelligence tools and JDBC drivers to connect to a Ballista cluster
- **Google Protocol Buffers** for serializing query plans.
- **Docker** for packaging up executors along with user-defined code.
- **Kubernetes** for deployment and management of the executor docker containers.

Ballista was donated to the Arrow project in 2021 and is not ready for production use although it is capable of running a number of queries from the popular TPC-H benchmark with good performance.

### C++ Query Engine

The C++ implementation has work in progress to add a query engine and the current focus is on implementing efficient compute primitives and a Dataset API.

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
