package io.bufferslayer;

import io.bufferslayer.Message.MessageKey;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by guohang.bao on 2017/2/28.
 */
public class InMemoryReporterMetrics extends ReporterMetrics {

  enum MetricKey {
    messages,
    messagesDropped
  }

  private static InMemoryReporterMetrics instance;

  final ConcurrentHashMap<MetricKey, AtomicLong> metrics = new ConcurrentHashMap<>();
  final ConcurrentHashMap<MessageKey, AtomicLong> queuedMessages = new ConcurrentHashMap<>();

  private final Lock lock = new ReentrantLock();

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
    if (quantity == 0) {
      return;
    }
    while (true) {
      AtomicLong metric = metrics.get(key);
      if (metric == null) {
        try {
          lock.lock();
          metric = metrics.get(key);
          if (metric == null) {
            metrics.put(key, new AtomicLong(quantity));
            return;
          }
        } finally {
          lock.unlock();
        }
      }
      while (true) {
        long prev = metric.get();
        if (metric.compareAndSet(prev, prev + quantity)) {
          return;
        }
      }
    }
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
    AtomicLong metric = metrics.get(key);
    return metric == null ? 0 : metric.get();
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
    long count = 0;
    for (AtomicLong queued: queuedMessages.values()) {
      count += queued.get();
    }
    return count;
  }

  @Override
  public void updateQueuedMessages(MessageKey key, int quantity) {
    AtomicLong metric = queuedMessages.get(key);
    if (metric == null) {
      try {
        lock.lock();
        metric = queuedMessages.get(key);
        if (metric == null) {
          queuedMessages.put(key, new AtomicLong(quantity));
          return;
        }
      } finally {
        lock.unlock();
      }
    }
    metric.set(quantity);
  }

  @Override
  public void removeQueuedMessages(MessageKey queueKey) {
    queuedMessages.remove(queueKey);
  }

  public void clear() {
    metrics.clear();
    queuedMessages.clear();
  }
}
