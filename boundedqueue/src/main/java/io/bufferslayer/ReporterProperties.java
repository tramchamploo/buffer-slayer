package io.bufferslayer;

import static io.bufferslayer.OverflowStrategy.Strategy.DropHead;

import io.bufferslayer.AsyncReporter.Builder;
import java.util.concurrent.TimeUnit;

/**
 * Created by tramchamploo on 2017/3/16.
 */
public class ReporterProperties {

  private Sender sender;
  private int senderThreads = 1;
  private String metrics = "noop";
  private String metricsExporter = "noop";
  private long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
  private long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
  private int flushThreads = 1;
  private int timerThreads = AsyncReporter.DEFAULT_TIMER_THREADS;
  private int bufferedMaxMessages = 100;
  private int pendingMaxMessages = 10000;
  private boolean strictOrder = false;
  private String overflowStrategy = DropHead.name();
  private String recycler = "sizefirst";

  public Sender getSender() {
    return sender;
  }

  public ReporterProperties setSender(Sender sender) {
    this.sender = sender;
    return this;
  }

  public int getSenderThreads() {
    return senderThreads;
  }

  public ReporterProperties setSenderThreads(int senderThreads) {
    this.senderThreads = senderThreads;
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

  public long getPendingKeepaliveNanos() {
    return pendingKeepaliveNanos;
  }

  public ReporterProperties setPendingKeepaliveNanos(long pendingKeepalive, TimeUnit unit) {
    this.pendingKeepaliveNanos = unit.toNanos(pendingKeepalive);
    return this;
  }

  public int getFlushThreads() {
    return flushThreads;
  }

  public ReporterProperties setFlushThreads(int flushThreads) {
    this.flushThreads = flushThreads;
    return this;
  }

  public int getTimerThreads() {
    return timerThreads;
  }

  public ReporterProperties setTimerThreads(int timerThreads) {
    this.timerThreads = timerThreads;
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
    this.metricsExporter = exporter;
    return this;
  }

  public String getMetricsExporter() {
    return this.metricsExporter;
  }

  public ReporterProperties setStrictOrder(boolean strictOrder) {
    this.strictOrder = strictOrder;
    return this;
  }

  public boolean isStrictOrder() {
    return strictOrder;
  }

  public String getOverflowStrategy() {
    return overflowStrategy;
  }

  public ReporterProperties setOverflowStrategy(String overflowStrategy) {
    this.overflowStrategy = overflowStrategy;
    return this;
  }

  public String getRecycler() {
    return recycler;
  }

  public ReporterProperties setRecycler(String recycler) {
    this.recycler = recycler;
    return this;
  }

  AsyncReporter.Builder toBuilder() {
    Builder builder = new Builder(sender)
        .senderThreads(senderThreads)
        .messageTimeout(messageTimeoutNanos, TimeUnit.NANOSECONDS)
        .pendingKeepalive(pendingKeepaliveNanos, TimeUnit.NANOSECONDS)
        .flushThreads(flushThreads)
        .timerThreads(timerThreads)
        .bufferedMaxMessages(bufferedMaxMessages)
        .pendingMaxMessages(pendingMaxMessages)
        .strictOrder(strictOrder)
        .overflowStrategy(OverflowStrategy.create(overflowStrategy))
        .recycler(recycler);
    if (metrics.equalsIgnoreCase("inmemory")) {
      builder.metrics(InMemoryReporterMetrics
          .instance(ReporterMetricsExporter.of(metricsExporter)));
    }
    return builder;
  }
}
