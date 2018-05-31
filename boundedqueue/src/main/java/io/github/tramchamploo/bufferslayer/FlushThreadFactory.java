package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.AsyncReporter.REPORTER_STATE_SHUTDOWN;
import static io.github.tramchamploo.bufferslayer.AsyncReporter.REPORTER_STATE_UPDATER;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;

/**
 * Produce threads that flush messages when buffer is full
 */
class FlushThreadFactory {

  static Logger logger = AsyncReporter.logger;

  final AsyncReporter<?, ?> reporter;
  final FlushSynchronizer synchronizer;
  final ThreadFactory factory;

  FlushThreadFactory(AsyncReporter<?, ?> reporter) {
    this.reporter = reporter;
    this.synchronizer = reporter.synchronizer;
    this.factory = new ThreadFactoryBuilder()
        .setNameFormat(reporter.getClass().getSimpleName() + "-" + reporter.id + "-flusher-%d")
        .setDaemon(true)
        .build();
  }

  Thread newFlushThread() {
    return factory.newThread(new FlushRunnable());
  }

  private void reScheduleAndFlush(AbstractSizeBoundedQueue q) {
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

  private class FlushRunnable implements Runnable {
    @Override
    public void run() {
      try {
        while (REPORTER_STATE_UPDATER.get(reporter) != REPORTER_STATE_SHUTDOWN) {
          // wait when no queue is ready
          AbstractSizeBoundedQueue q = synchronizer.poll(reporter.messageTimeoutNanos);

          if (Thread.interrupted()) {
            logger.warn("Interrupted while waiting for a ready queue");
            break;
          }

          if (q == null)
            continue;

          reScheduleAndFlush(q);
        }
      } finally {
        if (REPORTER_STATE_UPDATER.get(reporter) == REPORTER_STATE_SHUTDOWN) { // wake up cleanup thread
          reporter.close.countDown();
        }
      }
    }
  }
}
