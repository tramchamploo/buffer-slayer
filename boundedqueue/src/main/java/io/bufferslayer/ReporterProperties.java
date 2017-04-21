package io.bufferslayer;

import static io.bufferslayer.OverflowStrategy.Strategy.DropHead;

import com.google.common.util.concurrent.MoreExecutors;
import io.bufferslayer.AsyncReporter.Builder;
import io.bufferslayer.ReporterMetricsExporter.Exporters;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Created by guohang.bao on 2017/3/16.
 */
public class ReporterProperties {

  private Sender sender;
  private int parallelismPerBatch = 1;
  private Executor senderExecutor = MoreExecutors.directExecutor();
  private String metrics = "noop";
  private Exporters metricsExporter = Exporters.noop;
  private long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
  private long flushThreadKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
  private int bufferedMaxMessages = 100;
  private int pendingMaxMessages = 10000;
  private boolean strictOrder = false;
  private String overflowStrategy = DropHead.name();

  public Sender getSender() {
    return sender;
  }

  public ReporterProperties setSender(Sender sender) {
    this.sender = sender;
    return this;
  }

  public int getParallelismPerBatch() {
    return parallelismPerBatch;
  }

  public ReporterProperties setParallelismPerBatch(int parallelism) {
    this.parallelismPerBatch = parallelism;
    return this;
  }

  public Executor getSenderExecutor() {
    return senderExecutor;
  }

  public ReporterProperties setSenderExecutor(Executor executor) {
    this.senderExecutor = executor;
    return this;
  }

  public String getMetrics() {
    return metrics;
  }

  public ReporterProperties setMetrics(String metrics) {
    if (!metrics.equalsIgnoreCase("inmemory")) {
      throw new UnsupportedOperationException(metrics);
    }
    this.metrics = metrics;
    return this;
  }

  public long getMessageTimeoutNanos() {
    return messageTimeoutNanos;
  }

  public ReporterProperties setMessageTimeout(long messageTimeout, TimeUnit unit) {
    this.messageTimeoutNanos = unit.toNanos(messageTimeout);
    return this;
  }

  public long getFlushThreadKeepaliveNanos() {
    return flushThreadKeepaliveNanos;
  }

  public ReporterProperties setFlushThreadKeepaliveNanos(long flushThreadKeepalive, TimeUnit unit) {
    this.flushThreadKeepaliveNanos = unit.toNanos(flushThreadKeepalive);
    return this;
  }

  public int getBufferedMaxMessages() {
    return bufferedMaxMessages;
  }

  public ReporterProperties setBufferedMaxMessages(int bufferedMaxMessages) {
    this.bufferedMaxMessages = bufferedMaxMessages;
    return this;
  }

  public int getPendingMaxMessages() {
    return pendingMaxMessages;
  }

  public ReporterProperties setPendingMaxMessages(int pendingMaxMessages) {
    this.pendingMaxMessages = pendingMaxMessages;
    return this;
  }

  public ReporterProperties setMetricsExporter(String exporter) {
    if (exporter.equalsIgnoreCase(Exporters.http.name())) {
      this.metricsExporter = Exporters.http;
    } else if (exporter.equalsIgnoreCase(Exporters.log.name())) {
      this.metricsExporter = Exporters.log;
    } else {
      throw new UnsupportedOperationException(exporter);
    }
    return this;
  }

  public Exporters getMetricsExporter() {
    return this.metricsExporter;
  }

  public ReporterProperties setStrictOrder(boolean strictOrder) {
    this.strictOrder = strictOrder;
    return this;
  }

  public boolean isStrictOrder() {
    return strictOrder;
  }

  public ReporterProperties setOverflowStrategy(String overflowStrategy) {
    this.overflowStrategy = overflowStrategy;
    return this;
  }

  public String getOverflowStrategy() {
    return overflowStrategy;
  }

  public AsyncReporter.Builder toBuilder() {
    Builder builder = new Builder(sender)
        .senderExecutor(senderExecutor)
        .parallelismPerBatch(parallelismPerBatch)
        .messageTimeout(messageTimeoutNanos, TimeUnit.NANOSECONDS)
        .flushThreadKeepalive(flushThreadKeepaliveNanos, TimeUnit.NANOSECONDS)
        .bufferedMaxMessages(bufferedMaxMessages)
        .pendingMaxMessages(pendingMaxMessages)
        .strictOrder(strictOrder)
        .overflowStrategy(OverflowStrategy.create(overflowStrategy));
    if (metrics.equalsIgnoreCase("inmemory")) {
      builder.metrics(InMemoryReporterMetrics
          .instance(ReporterMetricsExporter.of(metricsExporter)));
    }
    return builder;
  }
}
