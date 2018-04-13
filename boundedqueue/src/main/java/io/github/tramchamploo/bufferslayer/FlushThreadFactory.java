package io.github.tramchamploo.bufferslayer;

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

  private class FlushRunnable implements Runnable {
    @Override
    public void run() {
      try {
        while (!reporter.closed.get()) {
          if (Thread.interrupted()) {
            logger.error("Interrupted while waiting for a ready queue");
            break;
          }
          // wait when no queue is ready
          SizeBoundedQueue q = synchronizer.poll(reporter.messageTimeoutNanos);
          if (q == null) continue;
          reScheduleAndFlush(q);
        }
      } finally {
        if (reporter.closed.get()) { // wake up cleanup thread
          reporter.close.countDown();
        }
      }
    }
  }
}
