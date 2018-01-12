package io.github.tramchamploo.bufferslayer;

import com.google.common.base.Preconditions;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Record total messages in queue
 */
abstract class MemoryLimiter {

  private static final int MAX_TOTAL_MESSGAES = 1_000_000;

  static MemoryLimiter maxOf(int maxMessages, ReporterMetrics metrics) {
    Preconditions.checkArgument(maxMessages > 0, "maxMessages should be greater than 0.");
    int result = Math.min(MAX_TOTAL_MESSGAES, maxMessages);
    return new DefaultMemoryLimiter(result, metrics);
  }

  abstract boolean isMaximum();

  abstract void waitWhenMaximum();

  abstract void signalAll();

  private static final class DefaultMemoryLimiter extends MemoryLimiter {
    private static final Logger logger = LoggerFactory.getLogger(AsyncReporter.class);

    private final long maxMessages;
    private final ReporterMetrics metrics;

    private final Lock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();

    private DefaultMemoryLimiter(long maxMessages, ReporterMetrics metrics) {
      this.maxMessages = maxMessages;
      this.metrics = metrics;
    }

    @Override
    boolean isMaximum() {
      return metrics.queuedMessages() >= maxMessages;
    }

    @Override
    void waitWhenMaximum() {
      lock.lock();
      try {
        while (isMaximum()) {
          notFull.await();
        }
      } catch (InterruptedException e) {
        logger.error("Interrupted waiting when full.");
      } finally {
        lock.unlock();
      }
    }

    @Override
    void signalAll() {
      lock.lock();
      try {
        notFull.signalAll();
      } finally {
        lock.unlock();
      }
    }
  }
}

