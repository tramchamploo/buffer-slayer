package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.chmv8.LongAdderV8;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics that keeps data in memory
 */
public class InMemoryReporterMetrics extends ReporterMetrics {

  enum MetricKey {
    messages,
    messagesDropped
  }

  private static InMemoryReporterMetrics instance;

  final ConcurrentHashMap<MetricKey, LongAdderV8> metrics = new ConcurrentHashMap<>();

  final ConcurrentHashMap<MessageKey, AtomicLong> queuedMessages = new ConcurrentHashMap<>();
  // Every update will affect this value, so get will not do calculations in real time
  private final LongAdderV8 queuedMessagesAccumulator = new LongAdderV8();

  private InMemoryReporterMetrics(ReporterMetricsExporter exporter) {
    startExporter(exporter);
  }

  public static InMemoryReporterMetrics instance(ReporterMetricsExporter exporter) {
    if (instance == null) {
      synchronized (InMemoryReporterMetrics.class) {
        if (instance == null) {
          instance = new InMemoryReporterMetrics(exporter);
        }
      }
    }
    return instance;
  }

  private void increment(MetricKey key, int quantity) {
    if (quantity == 0) return;

    LongAdderV8 metric = metrics.get(key);
    if (metric == null) {
      metric = new LongAdderV8();
      metric.add(quantity);
      metric = metrics.putIfAbsent(key, metric);
      if (metric == null) return;
    }
    metric.add(quantity);
  }

  @Override
  public void incrementMessages(int quantity) {
    increment(MetricKey.messages, quantity);
  }

  @Override
  public void incrementMessagesDropped(int quantity) {
    increment(MetricKey.messagesDropped, quantity);
  }

  private long get(MetricKey key) {
    LongAdderV8 metric = metrics.get(key);
    return metric == null ? 0 : metric.sum();
  }

  @Override
  public long messages() {
    return get(MetricKey.messages);
  }

  @Override
  public long messagesDropped() {
    return get(MetricKey.messagesDropped);
  }

  @Override
  public long queuedMessages() {
    return queuedMessagesAccumulator.sum();
  }

  @Override
  public void updateQueuedMessages(MessageKey key, int update) {
    AtomicLong metric = queuedMessages.get(key);
    if (metric == null) {
      metric = queuedMessages.putIfAbsent(key, new AtomicLong(update));
      if (metric == null) {
        queuedMessagesAccumulator.add(update);
        return;
      }
    }
    long prev = metric.getAndSet(update);
    queuedMessagesAccumulator.add(update - prev);
  }

  @Override
  public void removeFromQueuedMessages(MessageKey queueKey) {
    queuedMessages.remove(queueKey);
  }

  public void clear() {
    metrics.clear();
    queuedMessages.clear();
    queuedMessagesAccumulator.reset();
  }
}
