package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.AsyncReporter.Builder;
import java.util.concurrent.TimeUnit;

public class AsyncReporterProperties extends AbstractReporterProperties<AsyncReporterProperties> {

  private int sharedSenderThreads = 1;
  private long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
  private int flushThreads = AsyncReporter.DEFAULT_FLUSH_THREADS;
  private int timerThreads = AsyncReporter.DEFAULT_TIMER_THREADS;
  private boolean singleKey = false;
  private int totalQueuedMessages = 100_000;

  public int getSharedSenderThreads() {
    return sharedSenderThreads;
  }

  public AsyncReporterProperties setSharedSenderThreads(int sharedSenderThreads) {
    this.sharedSenderThreads = sharedSenderThreads;
    return self();
  }

  public long getPendingKeepaliveNanos() {
    return pendingKeepaliveNanos;
  }

  public AsyncReporterProperties setPendingKeepaliveNanos(long pendingKeepalive, TimeUnit unit) {
    this.pendingKeepaliveNanos = unit.toNanos(pendingKeepalive);
    return self();
  }

  public int getFlushThreads() {
    return flushThreads;
  }

  public AsyncReporterProperties setFlushThreads(int flushThreads) {
    this.flushThreads = flushThreads;
    return self();
  }

  public int getTimerThreads() {
    return timerThreads;
  }

  public AsyncReporterProperties setTimerThreads(int timerThreads) {
    this.timerThreads = timerThreads;
    return self();
  }

  public AsyncReporterProperties setSingleKey(boolean singleKey) {
    this.singleKey = singleKey;
    return self();
  }

  public boolean isSingleKey() {
    return singleKey;
  }

  public int getTotalQueuedMessages() {
    return totalQueuedMessages;
  }

  public AsyncReporterProperties setTotalQueuedMessages(int totalQueuedMessages) {
    this.totalQueuedMessages = totalQueuedMessages;
    return this;
  }

  public Builder toBuilder() {
    Builder builder = new Builder<>(sender).sharedSenderThreads(sharedSenderThreads)
                                           .messageTimeout(messageTimeoutNanos, TimeUnit.NANOSECONDS)
                                           .pendingKeepalive(pendingKeepaliveNanos, TimeUnit.NANOSECONDS)
                                           .flushThreads(flushThreads)
                                           .timerThreads(timerThreads)
                                           .bufferedMaxMessages(bufferedMaxMessages)
                                           .pendingMaxMessages(pendingMaxMessages)
                                           .singleKey(singleKey)
                                           .overflowStrategy(OverflowStrategy.create(overflowStrategy))
                                           .totalQueuedMessages(totalQueuedMessages);
    if (metrics.equalsIgnoreCase("inmemory")) {
      builder.metrics(InMemoryReporterMetrics.instance(ReporterMetricsExporter.of(metricsExporter)));
    }
    return builder;
  }
}
