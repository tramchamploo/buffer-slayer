package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.FiberReporter.Builder;
import java.util.concurrent.TimeUnit;

public class FiberReporterProperties extends AbstractReporterProperties<FiberReporterProperties> {

  private int senderThreads = 1;
  private long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
  private int flushThreads = 1;
  private int timerThreads = 1;
  private boolean singleKey = false;

  public int getSenderThreads() {
    return senderThreads;
  }

  public FiberReporterProperties setSenderThreads(int senderThreads) {
    this.senderThreads = senderThreads;
    return self();
  }

  public long getPendingKeepaliveNanos() {
    return pendingKeepaliveNanos;
  }

  public FiberReporterProperties setPendingKeepaliveNanos(long pendingKeepalive, TimeUnit unit) {
    this.pendingKeepaliveNanos = unit.toNanos(pendingKeepalive);
    return self();
  }

  public int getFlushThreads() {
    return flushThreads;
  }

  public FiberReporterProperties setFlushThreads(int flushThreads) {
    this.flushThreads = flushThreads;
    return self();
  }

  public int getTimerThreads() {
    return timerThreads;
  }

  public FiberReporterProperties setTimerThreads(int timerThreads) {
    this.timerThreads = timerThreads;
    return self();
  }

  public FiberReporterProperties setSingleKey(boolean singleKey) {
    this.singleKey = singleKey;
    return self();
  }

  public boolean isSingleKey() {
    return singleKey;
  }

  public Builder toBuilder() {
    Builder builder = new Builder<>(sender)
        .messageTimeout(messageTimeoutNanos, TimeUnit.NANOSECONDS)
        .pendingKeepalive(pendingKeepaliveNanos, TimeUnit.NANOSECONDS)
        .senderThreads(senderThreads)
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
