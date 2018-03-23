package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.concurrent.TimeUnit;

public abstract class AbstractReporterProperties implements ReporterProperties {

  protected Sender<? extends Message, ?> sender;
  protected String metrics = "noop";
  protected String metricsExporter = "noop";
  protected long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
  protected int bufferedMaxMessages = 100;
  protected int pendingMaxMessages = 10000;
  protected String overflowStrategy = Strategy.DropHead.name();

  public String getMetrics() {
    return metrics;
  }

  public AbstractReporterProperties setMetrics(String metrics) {
    if (!metrics.equalsIgnoreCase("inmemory")) {
      throw new UnsupportedOperationException(metrics);
    }
    this.metrics = metrics;
    return this;
  }

  public long getMessageTimeoutNanos() {
    return messageTimeoutNanos;
  }

  public AbstractReporterProperties setMessageTimeout(long messageTimeout, TimeUnit unit) {
    this.messageTimeoutNanos = unit.toNanos(messageTimeout);
    return this;
  }

  public int getBufferedMaxMessages() {
    return bufferedMaxMessages;
  }

  public AbstractReporterProperties setBufferedMaxMessages(int bufferedMaxMessages) {
    this.bufferedMaxMessages = bufferedMaxMessages;
    return this;
  }

  public int getPendingMaxMessages() {
    return pendingMaxMessages;
  }

  public AbstractReporterProperties setPendingMaxMessages(int pendingMaxMessages) {
    this.pendingMaxMessages = pendingMaxMessages;
    return this;
  }

  public String getMetricsExporter() {
    return metricsExporter;
  }

  public AbstractReporterProperties setMetricsExporter(String exporter) {
    this.metricsExporter = exporter;
    return this;
  }

  public String getOverflowStrategy() {
    return overflowStrategy;
  }

  public AbstractReporterProperties setOverflowStrategy(String overflowStrategy) {
    this.overflowStrategy = overflowStrategy;
    return this;
  }
}
