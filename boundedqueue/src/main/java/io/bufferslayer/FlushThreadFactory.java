package io.bufferslayer;

import static io.bufferslayer.DeferredHolder.newDeferred;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
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
        .setNameFormat("AsyncReporter-" + reporter.id + "-flush-thread-%d")
        .setDaemon(true)
        .build();
  }

  Thread newFlushThread() {
    final BufferNextMessage consumer = new BufferNextMessage(reporter.bufferedMaxMessages,
        reporter.messageTimeoutNanos, reporter.strictOrder);
    Thread flushThread = factory.newThread(new Runnable() {
      @Override
      public void run() {
        try {
          while (!reporter.closed.get()) {
            SizeBoundedQueue q = leaseQueue();
            if (q == null) continue;
            try {
              reporter.flush(q, consumer);
            } finally {
              reporter.pendingRecycler.recycle(q);
            }
          }
        } finally {
          // flush messages left in buffer
          List<Message> drained = consumer.drain();
          SizeBoundedQueue q;
          if (drained.size() > 0 && (q = leaseQueue()) != null) {
            for (Message message : drained) q.offer(message, newDeferred(message.id));
            reporter.flushTillEmpty(q);
          }
          // wake up notice thread
          if (reporter.closed.get()) reporter.close.countDown();
        }
      }
    });

    return flushThread;
  }

  private SizeBoundedQueue leaseQueue() {
    return reporter.pendingRecycler.lease(reporter.messageTimeoutNanos, TimeUnit.NANOSECONDS);
  }
}
