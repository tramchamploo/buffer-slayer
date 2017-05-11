[![CircleCI](https://circleci.com/gh/tramchamploo/buffer-slayer.svg?style=svg)](https://circleci.com/gh/tramchamploo/buffer-slayer)
[ ![Download](https://api.bintray.com/packages/tramchamploo/tramchamploo/buffer-slayer/images/download.svg) ](https://bintray.com/tramchamploo/tramchamploo/buffer-slayer/_latestVersion)

# buffer-slayer
buffer-slayer is tool that buffers requests and send them in batch, of which sender supports batch operation. Such as `Spring-JdbcTemplate`(batchUpdate), `Redis`(pipeline).

It has a queue that allows multiple producers to send to, and it is also limited so to keep application away from Overflowing. 

Also there is a buffer that has a fixed size of which content is send to consumer. The buffer will send data when it is full or a specific timeout is reached whichever comes first.

This project is inspired by [zipkin-reporter-java](https://github.com/openzipkin/zipkin-reporter-java).
 
## Motivation
* Consumer is always faster in batch than consuming one by one.
* When consumer is slower than producer, don't overflow application's memory.
* If a flood of requests is coming, low down the impact on the backing storage (DB, redis, etc.).
* A promise is returned for async sending. Even though messages are sent in batch, you can get a one-to-one promise from message to sending result.

## Usage

### [ReporterProperties](boundedqueue/src/main/java/io/bufferslayer/ReporterProperties.java)
This is where you configure all properties.

* `sender`: Sender that messages are flushed into. Necessary but often not needed for users to configure. Implementations like `JdbcTemplate` will configure it by itself.
* `metrics`: (inmemory, noop) metrics that records nums of sent, dropped, queued messages.
* `metricsExporter`: (http, log) exporter to let users know data of metrics.
* `bufferedMaxMessages`: Max size of buffer that sent in one batch.
* `messageTimeoutNanos`: If buffer size is not reached, flush will be invoked after this timeout.
* `pendingMaxMessages`: Max size of messages to be stashed until OverflowStrategy is triggered.
* `pendingKeepaliveNanos`: Pending queue should die if no messages queued into during in its keepalive.
* `flushThreadCount`: Num of threads that flush messages to sender.
* `overflowStrategy`: (DropHead, DropTail, DropBuffer, DropNew, Fail) after pendingMaxMessages is reached, the strategy will be triggered.
* `senderExecutor`: Executor for sender to run with.
* `parallelismPerBatch`: `bufferedMaxMessages` of messages can be divided into pieces and executed in parallel in `senderExecutor`.
* `strictOrder`: Whether the messages be sent in order. Notice that if this value is true, different kinds of messages will be staged in the same `SizeBoundedQueue`.

## JdbcTemplate
[`sql-buffer-jdbc`](/jdbc) is a buffer implementation of Spring's JdbcTemplate.

The query is forwarded to the delegated JdbcTemplate itself. 
The update directly goes to the reporter and returns a Promise.

### Quick-start
```xml
<dependency>
  <groupId>io.bufferslayer</groupId>
  <artifactId>buffer-spring-jdbc</artifactId>
  <version>1.1.2</version>
</dependency>
```

```java
ReporterProperties reporterProperties = new ReporterProperties()
        .setBufferedMaxMessages(500)
        .setPendingMaxMessages(10000)
        .setMetrics("inmemory")
        .setMetricsExporter("http")
        .setParallelismPerBatch(5)
        .setSenderExecutor(new ThreadPoolExecutor(200,
            200, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>()));

BatchJdbcTemplate template = new BatchJdbcTemplate(reporterProperties);
template.setDataSource(dataSource);

Promise promise = template.update(...);
promise.done(success -> ...)
       .fail(reject -> ...);
```

### Benchmark
Here is a simple jdbc benchmark result on my MacBook Pro (Retina, 13-inch, Late 2013).

Using mysql 5.7.18, keeps executing a simple `INSERT INTO test.benchmark(data, time) VALUES(?, ?);`

```
Benchmark                                                      Mode  Cnt       Score  Units
BatchedJdbcTemplateBenchmark.high_contention_batched          thrpt   15    8709.042  ops/s
BatchedJdbcTemplateBenchmark.high_contention_unbatched        thrpt   15     271.529  ops/s
BatchedJdbcTemplateBenchmark.mild_contention_batched          thrpt   15    2146.595  ops/s
BatchedJdbcTemplateBenchmark.mild_contention_unbatched        thrpt   15     262.621  ops/s
BatchedJdbcTemplateBenchmark.no_contention_batched            thrpt   15    1194.852  ops/s
BatchedJdbcTemplateBenchmark.no_contention_unbatched          thrpt   15     201.806  ops/s
```

## Components

### [Reporter](core/src/main/java/io/bufferslayer/Reporter.java)
It sends requests to a queue and keeps flushing them to consumer.

### [Sender](core/src/main/java/io/bufferslayer/Sender.java)
Sending the messages that the buffer drained in batch.

### [SizeBoundedQueue](boundedqueue/src/main/java/io/bufferslayer/SizeBoundedQueue.java)
A queue that bounded by a specific size. Supports multi producers in parallel. It supports overflow strategies as listed.

* `DropHead`: drops the oldest element
* `DropTail`: drops the youngest element
* `DropBuffer`: drops all the buffered elements
* `DropNew`: drops the new element
* `Block`: block offer thread, this can be used as a simple back-pressure strategy
* `Fail`: throws an exception

Strategies above are inspired by Akka stream. 

### [BufferNextMessage](boundedqueue/src/main/java/io/bufferslayer/BufferNextMessage.java)
A list with a fixed size that can only be drained when a timeout is reached or is full.