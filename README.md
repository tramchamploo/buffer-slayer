[![CircleCI](https://circleci.com/gh/tramchamploo/buffer-slayer.svg?style=svg)](https://circleci.com/gh/tramchamploo/buffer-slayer)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.tramchamploo/bufferslayer-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.tramchamploo/bufferslayer-core)
[ ![Download](https://api.bintray.com/packages/tramchamploo/tramchamploo/buffer-slayer/images/download.svg) ](https://bintray.com/tramchamploo/tramchamploo/buffer-slayer/_latestVersion)

# buffer-slayer
buffer-slayer is tool that buffers requests and send them in batch, of which client supports batch operation. Such as `Spring-JdbcTemplate`(batchUpdate), `Redis`(pipeline).

It has a queue that allows multiple producers to send to, and limited so to keep application away from Overflowing. 

Also there is a fixed sized buffer to normalize data transportation. The buffer's data will be sent when it is full or a specific timeout is reached whichever comes first.

This project is inspired by [zipkin-reporter-java](https://github.com/openzipkin/zipkin-reporter-java).
 
## Motivation
* Consumer is always faster in batch than accepting one by one.
* When consumer is slower than producer, don't overflow application's memory.
* If a flood of requests is coming, low down the impact on the backing storage (DB, redis, etc.).
* A promise is returned for async sending. Even though messages are sent in batch, you can get a one-to-one promise from the message sent to sending result.

## JdbcTemplate
[`bufferslayer-spring-jdbc`](/jdbc) is a buffer implementation of Spring's JdbcTemplate.

Queries are forwarded to the delegated JdbcTemplate and executed blockingly.

Updates directly goes to the reporter and returns a `Promise` immediately.

### Quick-start
```xml
<dependency>
  <groupId>io.github.tramchamploo</groupId>
  <artifactId>bufferslayer-spring-jdbc</artifactId>
  <version>1.2.18</version>
</dependency>
```

```java
ReporterProperties reporterProperties = new ReporterProperties()
        .setFlushThreads(5)
        .setSenderThreads(10)
        .setBufferedMaxMessages(500)
        .setPendingMaxMessages(10000)
        .setMetrics("inmemory")
        .setMetricsExporter("http");

BatchJdbcTemplate template = new BatchJdbcTemplate(reporterProperties);
template.setDataSource(dataSource);

Promise promise = template.update(...);
promise.done(success -> ...)
       .fail(reject -> ...);
```

## Usage

### [ReporterProperties](boundedqueue/src/main/java/io/github/tramchamploo/bufferslayer/ReporterProperties.java)
This is where you configure all properties.

* `sender`: Sender that messages are flushed into. Necessary but often not needed for users to configure. Implementations like `JdbcTemplate` will configure it by itself.
* `senderThreads`: Num of threads that sender execute in.
* `timerThreads`: Num of threads in scheduled executor, flushing messages at a fixed rate.
* `flushThreads`: Num of threads that flush messages to sender. They wait until a buffer is full.
* `metrics`: (inmemory, noop) metrics that records nums of sent, dropped, queued messages.
* `metricsExporter`: (http, log) exporter to let users know data of metrics.
* `bufferedMaxMessages`: Max size of buffer that sent in one batch.
* `messageTimeoutNanos`: If buffer size is not reached, flush will be invoked after this timeout.
* `pendingMaxMessages`: Max size of messages to be stashed until OverflowStrategy is triggered.
* `pendingKeepaliveNanos`: Pending queue should die if no messages queued into during in its keepalive.
* `overflowStrategy`: (DropHead, DropTail, DropBuffer, DropNew, Fail) after pendingMaxMessages is reached, the strategy will be triggered.
* `singleKey`: If this value is true, different kinds of messages will be staged in the same `SizeBoundedQueue`.

## Benchmark
Here is a simple jdbc benchmark result on my MacBook Pro (Retina, 13-inch, Late 2013).

Using mysql 5.7.18, keeps executing a simple `INSERT INTO test.benchmark(data, time) VALUES(?, ?);`

```
Benchmark                                                    Mode  Cnt       Score  Units
BatchJdbcTemplateBenchmark.high_contention_batched          thrpt   15   27917.086  ops/s
BatchJdbcTemplateBenchmark.high_contention_unbatched        thrpt   15     316.562  ops/s
BatchJdbcTemplateBenchmark.mild_contention_batched          thrpt   15   18672.057  ops/s
BatchJdbcTemplateBenchmark.mild_contention_unbatched        thrpt   15     280.970  ops/s
BatchJdbcTemplateBenchmark.no_contention_batched            thrpt   15    9053.288  ops/s
BatchJdbcTemplateBenchmark.no_contention_unbatched          thrpt   15     198.581  ops/s
```

## Components

### [Reporter](core/src/main/java/io/github/tramchamploo/bufferslayer/Reporter.java)
It sends requests to a queue and keeps flushing them to consumer.

### [Sender](core/src/main/java/io/github/tramchamploo/bufferslayer/Sender.java)
Sending the messages that the buffer drained in batch.

### [SizeBoundedQueue](boundedqueue/src/main/java/io/github/tramchamploo/bufferslayer/SizeBoundedQueue.java)
A queue that bounded by a specific size. Supports multi producers in parallel. 
It supports overflow strategies as listed.

* `DropHead`: drops the oldest element
* `DropTail`: drops the youngest element
* `DropBuffer`: drops all the buffered elements
* `DropNew`: drops the new element
* `Block`: block offer thread, this can be used as a simple back-pressure strategy
* `Fail`: throws an exception

Strategies above are inspired by Akka stream. 

### [QueueManager](boundedqueue/src/main/java/io/github/tramchamploo/bufferslayer/QueueManager.java)
Manages `SizeBoundedQueue`'s lifecycle. 
Be responsible for queue creation and destroy.

### [Buffer](boundedqueue/src/main/java/io/github/tramchamploo/bufferslayer/Buffer.java)
A list with a fixed size that can only be drained when a timeout is reached or is full.