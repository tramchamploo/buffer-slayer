package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.AsyncReporter.Builder;
import java.util.concurrent.TimeUnit;

public class AsyncReporterProperties extends AbstractReporterProperties {

  static final int DEFAULT_TIMER_THREADS = 1;
  static final int DEFAULT_FLUSH_THREADS = 1;

  private int sharedSenderThreads = 1;
  private long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
  private long tickDurationNanos = TimeUnit.MILLISECONDS.toNanos(100);
  private int ticksPerWheel = 512;
  private int flushThreads = DEFAULT_FLUSH_THREADS;
  private int timerThreads = DEFAULT_TIMER_THREADS;
  private boolean singleKey = false;
  private int totalQueuedMessages = 100_000;

  @Override
  public AsyncReporterProperties setMetrics(String metrics) {
    super.setMetrics(metrics);
    return this;
  }

  @Override
  public AsyncReporterProperties setMessageTimeout(long messageTimeout, TimeUnit unit) {
    super.setMessageTimeout(messageTimeout, unit);
    return this;
  }

  @Override
  public AsyncReporterProperties setBufferedMaxMessages(int bufferedMaxMessages) {
    super.setBufferedMaxMessages(bufferedMaxMessages);
    return this;
  }

  @Override
  public AsyncReporterProperties setPendingMaxMessages(int pendingMaxMessages) {
    super.setPendingMaxMessages(pendingMaxMessages);
    return this;
  }

  @Override
  public AsyncReporterProperties setMetricsExporter(String exporter) {
    super.setMetricsExporter(exporter);
    return this;
  }

  @Override
  public AsyncReporterProperties setOverflowStrategy(String overflowStrategy) {
    super.setOverflowStrategy(overflowStrategy);
    return this;
  }

  public int getSharedSenderThreads() {
    return sharedSenderThreads;
  }

  public AsyncReporterProperties setSharedSenderThreads(int sharedSenderThreads) {
    this.sharedSenderThreads = sharedSenderThreads;
    return this;
  }

  public long getPendingKeepaliveNanos() {
    return pendingKeepaliveNanos;
  }

  public AsyncReporterProperties setPendingKeepalive(long pendingKeepalive, TimeUnit unit) {
    this.pendingKeepaliveNanos = unit.toNanos(pendingKeepalive);
    return this;
  }

  public long getTickDurationNanos() {
    return tickDurationNanos;
  }

  public AsyncReporterProperties setTickDuration(long tickDuration, TimeUnit unit) {
    this.tickDurationNanos = unit.toNanos(tickDuration);
    return this;
  }

  public int getTicksPerWheel() {
    return ticksPerWheel;
  }

  public AsyncReporterProperties setTicksPerWheel(int ticksPerWheel) {
    this.ticksPerWheel = ticksPerWheel;
    return this;
  }

  public int getFlushThreads() {
    return flushThreads;
  }

  public AsyncReporterProperties setFlushThreads(int flushThreads) {
    this.flushThreads = flushThreads;
    return this;
  }

  public int getTimerThreads() {
    return timerThreads;
  }

  public AsyncReporterProperties setTimerThreads(int timerThreads) {
    this.timerThreads = timerThreads;
    return this;
  }

  public AsyncReporterProperties setSingleKey(boolean singleKey) {
    this.singleKey = singleKey;
    return this;
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

  @Override
  public <M extends Message, R> Builder<M, R> toBuilder(Sender<M, R> sender) {
    Builder<M, R> builder = new Builder<>(sender).sharedSenderThreads(sharedSenderThreads)
        .messageTimeout(messageTimeoutNanos, TimeUnit.NANOSECONDS)
        .pendingKeepalive(pendingKeepaliveNanos, TimeUnit.NANOSECONDS)
        .tickDuration(tickDurationNanos, TimeUnit.NANOSECONDS)
        .ticksPerWheel(ticksPerWheel)
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
