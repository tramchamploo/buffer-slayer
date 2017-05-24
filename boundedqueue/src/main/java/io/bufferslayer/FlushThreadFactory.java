package io.bufferslayer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by tramchamploo on 2017/5/18.
 */
class FlushThreadFactory {

  final AsyncReporter reporter;
  final ThreadFactory factory;

  FlushThreadFactory(AsyncReporter reporter) {
    this.reporter = reporter;
    factory = new ThreadFactoryBuilder()
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
            SizeBoundedQueue q = leaseQueue();
            if (q == null) continue;
            try {
              if (q.size() >= reporter.bufferedMaxMessages) {
                if (reporter.scheduler != null) {
                  reporter.schedulePeriodically(q.key, reporter.messageTimeoutNanos);
                }
                reporter.flush(q);
              }
            } finally {
              reporter.pendingRecycler.recycle(q);
            }
          }
        } finally {
          // wake up notice thread
          if (reporter.closed.get()) reporter.close.countDown();
        }
      }
    });
  }

  private SizeBoundedQueue leaseQueue() {
    return reporter.pendingRecycler.lease(reporter.messageTimeoutNanos, TimeUnit.NANOSECONDS);
  }
}
