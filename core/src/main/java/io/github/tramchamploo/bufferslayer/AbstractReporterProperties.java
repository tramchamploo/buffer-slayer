package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.concurrent.TimeUnit;

public abstract class AbstractReporterProperties<T extends AbstractReporterProperties> implements ReporterProperties<T> {

  protected Sender<? extends Message, ?> sender;
  protected String metrics = "noop";
  protected String metricsExporter = "noop";
  protected long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
  protected int bufferedMaxMessages = 100;
  protected int pendingMaxMessages = 10000;
  protected String overflowStrategy = Strategy.DropHead.name();

  public Sender<? extends Message, ?> getSender() {
    return sender;
  }

  public T setSender(Sender<? extends Message, ?> sender) {
    this.sender = sender;
    return self();
  }

  public String getMetrics() {
    return metrics;
  }

  public T setMetrics(String metrics) {
    if (!metrics.equalsIgnoreCase("inmemory")) {
      throw new UnsupportedOperationException(metrics);
    }
    this.metrics = metrics;
    return self();
  }

  public long getMessageTimeoutNanos() {
    return messageTimeoutNanos;
  }

  public T setMessageTimeout(long messageTimeout, TimeUnit unit) {
    this.messageTimeoutNanos = unit.toNanos(messageTimeout);
    return self();
  }

  public int getBufferedMaxMessages() {
    return bufferedMaxMessages;
  }

  public T setBufferedMaxMessages(int bufferedMaxMessages) {
    this.bufferedMaxMessages = bufferedMaxMessages;
    return self();
  }

  public int getPendingMaxMessages() {
    return pendingMaxMessages;
  }

  public T setPendingMaxMessages(int pendingMaxMessages) {
    this.pendingMaxMessages = pendingMaxMessages;
    return self();
  }

  public T setMetricsExporter(String exporter) {
    this.metricsExporter = exporter;
    return self();
  }

  public String getMetricsExporter() {
    return metricsExporter;
  }

  public String getOverflowStrategy() {
    return overflowStrategy;
  }

  public T setOverflowStrategy(String overflowStrategy) {
    this.overflowStrategy = overflowStrategy;
    return self();
  }

  @SuppressWarnings("unchecked")
  T self() {
    return (T) this;
  }

  public abstract Reporter.Builder toBuilder();
}
