package io.bufferslayer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;

/**
 * Created by tramchamploo on 2017/5/18.
 */
class FlushThreadFactory {

  static Logger logger = AsyncReporter.logger;

  final AsyncReporter reporter;
  final FlushSynchronizer synchronizer;
  final ThreadFactory factory;

  FlushThreadFactory(AsyncReporter reporter) {
    this.reporter = reporter;
    this.synchronizer = reporter.synchronizer;
    this.factory = new ThreadFactoryBuilder()
        .setNameFormat("AsyncReporter-" + reporter.id + "-flusher-%d")
        .setDaemon(true)
        .build();
  }

  Thread newFlushThread() {
    return factory.newThread(new Runnable() {
      @Override
      public void run() {
        try {
          while (!reporter.closed.get()) {
            try {
              // wait when no queue is ready
              SizeBoundedQueue q = synchronizer.poll(reporter.messageTimeoutNanos);
              if (q == null) continue;
              reScheduleAndFlush(q);
            } catch (InterruptedException e) {
              logger.error("Interrupted waiting for a ready queue.");
            }
          }
        } finally {
          if (reporter.closed.get()) { // wake up notice thread
            reporter.close.countDown();
          }
        }
      }
    });
  }

  private void reScheduleAndFlush(SizeBoundedQueue q) {
    try {
      while (q.size() >= reporter.bufferedMaxMessages) {
        // cancel timer on the queue and reschedule
        if (reporter.scheduler != null) {
          reporter.schedulePeriodically(q.key, reporter.messageTimeoutNanos);
        }
        reporter.flush(q);
      }
    } finally {
      synchronizer.release(q);
    }
  }
}
