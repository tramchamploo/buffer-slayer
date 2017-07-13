package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.AsyncReporter.Builder;
import java.util.concurrent.TimeUnit;

public class AsyncReporterProperties extends AbstractReporterProperties<AsyncReporterProperties> {

  private int senderThreads = 1;
  private long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
  private int flushThreads = 1;
  private int timerThreads = AsyncReporter.DEFAULT_TIMER_THREADS;
  private boolean singleKey = false;

  public int getSenderThreads() {
    return senderThreads;
  }

  public AsyncReporterProperties setSenderThreads(int senderThreads) {
    this.senderThreads = senderThreads;
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

  public Builder toBuilder() {
    Builder builder = new Builder<>(sender).senderThreads(senderThreads)
                                           .messageTimeout(messageTimeoutNanos, TimeUnit.NANOSECONDS)
                                           .pendingKeepalive(pendingKeepaliveNanos, TimeUnit.NANOSECONDS)
                                           .flushThreads(flushThreads)
                                           .timerThreads(timerThreads)
                                           .bufferedMaxMessages(bufferedMaxMessages)
                                           .pendingMaxMessages(pendingMaxMessages)
                                           .singleKey(singleKey)
                                           .overflowStrategy(OverflowStrategy.create(overflowStrategy));
    if (metrics.equalsIgnoreCase("inmemory")) {
      builder.metrics(InMemoryReporterMetrics.instance(ReporterMetricsExporter.of(metricsExporter)));
    }
    return builder;
  }
}
