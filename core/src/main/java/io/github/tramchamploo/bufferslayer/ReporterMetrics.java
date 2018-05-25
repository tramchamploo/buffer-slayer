package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;

/**
 * Metrics of reporter
 */
public abstract class ReporterMetrics {

  void startExporter(final ReporterMetricsExporter exporter) {
    exporter.start(this);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        exporter.close();
      }
    });
  }

  public abstract void incrementMessages(int quantity);

  public abstract void incrementMessagesDropped(int quantity);

  public abstract long messages();

  public abstract long messagesDropped();

  public abstract long queuedMessages();

  public abstract void updateQueuedMessages(MessageKey key, int quantity);

  public abstract void removeFromQueuedMessages(MessageKey key);

  public static final ReporterMetrics NOOP_METRICS = new ReporterMetrics() {
    @Override
    public void incrementMessages(int quantity) {
    }

    @Override
    public void incrementMessagesDropped(int quantity) {
    }

    @Override
    public long messages() {
      return 0;
    }

    @Override
    public long messagesDropped() {
      return 0;
    }

    @Override
    public long queuedMessages() {
      return 0;
    }

    @Override
    public void updateQueuedMessages(MessageKey key, int quantity) {
    }

    @Override
    public void removeFromQueuedMessages(MessageKey key) {
    }
  };
}
