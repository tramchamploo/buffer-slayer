package io.github.tramchamploo.bufferslayer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tramchamploo on 2017/3/16.
 */
public class LogReporterMetricsExporter extends ReporterMetricsExporter {

  private static final Logger logger = LoggerFactory.getLogger(LogReporterMetricsExporter.class);
  private ScheduledExecutorService executor;

  @Override
  public void start(final ReporterMetrics metrics) {
    executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        logger.info("Messages: {}\nMessagesDropped: {}\nQueuedMessages: {}",
            metrics.messages(), metrics.messagesDropped(), metrics.queuedMessages());
      }
    }, 5, 5, TimeUnit.SECONDS);
  }

  @Override
  public void close() {
    if (executor != null) executor.shutdown();
  }
}
