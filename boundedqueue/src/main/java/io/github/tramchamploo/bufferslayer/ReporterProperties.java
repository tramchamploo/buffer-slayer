package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.AsyncReporter.Builder;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.concurrent.TimeUnit;

/**
 * Created by tramchamploo on 2017/3/16.
 */
public class ReporterProperties<M extends Message, R> {

  private Sender<M, R> sender;
  private int senderThreads = 1;
  private String metrics = "noop";
  private String metricsExporter = "noop";
  private long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
  private long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
  private int flushThreads = 1;
  private int timerThreads = AsyncReporter.DEFAULT_TIMER_THREADS;
  private int bufferedMaxMessages = 100;
  private int pendingMaxMessages = 10000;
  private boolean singleKey = false;
  private String overflowStrategy = Strategy.DropHead.name();

  public Sender<M, R> getSender() {
    return sender;
  }

  public ReporterProperties<M, R> setSender(Sender<M, R> sender) {
    this.sender = sender;
    return this;
  }

  public int getSenderThreads() {
    return senderThreads;
  }

  public ReporterProperties<M, R> setSenderThreads(int senderThreads) {
    this.senderThreads = senderThreads;
    return this;
  }

  public String getMetrics() {
    return metrics;
  }

  public ReporterProperties<M, R> setMetrics(String metrics) {
    if (!metrics.equalsIgnoreCase("inmemory")) {
      throw new UnsupportedOperationException(metrics);
    }
    this.metrics = metrics;
    return this;
  }

  public long getMessageTimeoutNanos() {
    return messageTimeoutNanos;
  }

  public ReporterProperties<M, R> setMessageTimeout(long messageTimeout, TimeUnit unit) {
    this.messageTimeoutNanos = unit.toNanos(messageTimeout);
    return this;
  }

  public long getPendingKeepaliveNanos() {
    return pendingKeepaliveNanos;
  }

  public ReporterProperties<M, R> setPendingKeepaliveNanos(long pendingKeepalive, TimeUnit unit) {
    this.pendingKeepaliveNanos = unit.toNanos(pendingKeepalive);
    return this;
  }

  public int getFlushThreads() {
    return flushThreads;
  }

  public ReporterProperties<M, R> setFlushThreads(int flushThreads) {
    this.flushThreads = flushThreads;
    return this;
  }

  public int getTimerThreads() {
    return timerThreads;
  }

  public ReporterProperties<M, R> setTimerThreads(int timerThreads) {
    this.timerThreads = timerThreads;
    return this;
  }

  public int getBufferedMaxMessages() {
    return bufferedMaxMessages;
  }

  public ReporterProperties<M, R> setBufferedMaxMessages(int bufferedMaxMessages) {
    this.bufferedMaxMessages = bufferedMaxMessages;
    return this;
  }

  public int getPendingMaxMessages() {
    return pendingMaxMessages;
  }

  public ReporterProperties<M, R> setPendingMaxMessages(int pendingMaxMessages) {
    this.pendingMaxMessages = pendingMaxMessages;
    return this;
  }

  public ReporterProperties<M, R> setMetricsExporter(String exporter) {
    this.metricsExporter = exporter;
    return this;
  }

  public String getMetricsExporter() {
    return this.metricsExporter;
  }

  public ReporterProperties<M, R> setSingleKey(boolean singleKey) {
    this.singleKey = singleKey;
    return this;
  }

  public boolean isSingleKey() {
    return singleKey;
  }

  public String getOverflowStrategy() {
    return overflowStrategy;
  }

  public ReporterProperties<M, R> setOverflowStrategy(String overflowStrategy) {
    this.overflowStrategy = overflowStrategy;
    return this;
  }

  public AsyncReporter.Builder<M, R> toBuilder() {
    Builder<M, R> builder = new Builder<>(sender)
        .senderThreads(senderThreads)
        .messageTimeout(messageTimeoutNanos, TimeUnit.NANOSECONDS)
        .pendingKeepalive(pendingKeepaliveNanos, TimeUnit.NANOSECONDS)
        .flushThreads(flushThreads)
        .timerThreads(timerThreads)
        .bufferedMaxMessages(bufferedMaxMessages)
        .pendingMaxMessages(pendingMaxMessages)
        .singleKey(singleKey)
        .overflowStrategy(OverflowStrategy.create(overflowStrategy));
    if (metrics.equalsIgnoreCase("inmemory")) {
      builder.metrics(InMemoryReporterMetrics
          .instance(ReporterMetricsExporter.of(metricsExporter)));
    }
    return builder;
  }
}
