# Introduction

Since starting my first job in software engineering, I have been fascinated with databases and query languages. It seems almost magical to ask a computer a question and get meaningful data back efficiently. After many years of working as a generalist software developer and an end-user of data technologies, I started working for a startup that threw me into the deep end of distributed database development. This is the book that I wish had existed when I started on this journey. Although this is only an introductory-level book, I hope to demystify how query engines work.

My interest in query engines eventually led to me becoming involved in the Apache Arrow project, where I donated the initial Rust implementation in 2018, then donated the DataFusion in-memory query engine in 2019, and finally, donated the Ballista distributed compute project in 2021. I do not plan on building anything else outside the Arrow project and am now continuing to contribute to these projects within Arrow.

The Arrow project now has many active committers and contributors working on the Rust implementation, and it has improved significantly compared to my initial contribution.

Although Rust is a great choice for a high-performance query engine, it is not ideal for teaching the concepts around query engines, so I recently built a new query engine, implemented in Kotlin, as I was writing this book. Kotlin is a very concise language and easy to read, making it possible to include source code examples in this book. I would encourage you to get familiar with the source code as you work your way through this book and consider making some contributions. There is no better way to learn than to get some hands-on experience!

The query engine covered in this book was originally intended to be part of the Ballista project (and was for a while) but as the project evolved, it became apparent that it would make more sense to keep the query engine in Rust and support Java, and other languages, through a UDF mechanism rather than duplicating large amounts of query execution logic in multiple languages.

Now that Ballista has been donated to Apache Arrow, I have updated this book to refer to the query engine in the companion repository simply as "KQuery", short for Kotlin Query Engine, but if anyone has suggestions for a better name, please let me know!

Updates to this book will be made available free of charge as they become available, so please check back occasionally or follow me on Twitter (@andygrove_io) to receive notifications when new content is available.

## Feedback

Please send me a DM on Twitter at @andygrove_io or send an email to agrove@apache.org if you have any feedback on this book.

*This book is also available for purchase in ePub, MOBI, and PDF format from [https://leanpub.com/how-query-engines-work](https://leanpub.com/how-query-engines-work)*

**Copyright © 2020-2023 Andy Grove. All rights reserved.**
