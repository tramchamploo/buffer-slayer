package io.bufferslayer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/**
 * Created by tramchamploo on 2017/5/18.
 */
class FlushThreadFactory {

  static Logger logger = AsyncReporter.logger;

  final AsyncReporter reporter;
  final FlushSynchronizer synchronizer;
  final QueueRecycler recycler;
  final ThreadFactory factory;

  FlushThreadFactory(AsyncReporter reporter) {
    this.reporter = reporter;
    this.synchronizer = reporter.synchronizer;
    this.recycler = reporter.pendingRecycler;
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
              synchronizer.await(reporter.messageTimeoutNanos);
              while (synchronizer.remaining() > 0) {
                SizeBoundedQueue q = leaseQueue();
                if (q == null) continue;
                reScheduleAndFlush(q);
              }
            } catch (InterruptedException e) {
              logger.error("Interrupted waiting for a ready queue.");
            }
          }
        } finally {
          if (reporter.closed.get()) // wake up notice thread
            reporter.close.countDown();
        }
      }
    });
  }

  private SizeBoundedQueue leaseQueue() {
    return recycler.lease(reporter.messageTimeoutNanos, TimeUnit.NANOSECONDS);
  }

  private void reScheduleAndFlush(SizeBoundedQueue q) {
    try {
      if (q.size() >= reporter.bufferedMaxMessages) {
        if (reporter.scheduler != null) {
          reporter.schedulePeriodically(q.key, reporter.messageTimeoutNanos);
        }
        reporter.flush(q);
      }
    } finally {
      recycler.recycle(q);
    }
  }
}
