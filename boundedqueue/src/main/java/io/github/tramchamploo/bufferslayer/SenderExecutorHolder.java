package io.github.tramchamploo.bufferslayer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 * Holds singleton of sender executor
 */
class SenderExecutorHolder {

  private Executor executor;
  private final int sharedSenderThreads;
  private int refCount = 1;

  /** The name of the system property for setting the thread priority for this Scheduler. */
  private static final String KEY_IO_PRIORITY = "bufferslayer.io-priority";

  private static final int priority = Math.max(Thread.MIN_PRIORITY, Math.min(Thread.MAX_PRIORITY,
      Integer.getInteger(KEY_IO_PRIORITY, Thread.NORM_PRIORITY)));

  private static final ThreadFactory threadFactory = new ThreadFactoryBuilder()
      .setNameFormat("AsyncReporter-sender-%d")
      .setDaemon(true)
      .setPriority(priority)
      .build();

  SenderExecutorHolder(int sharedSenderThreads) {
    this.sharedSenderThreads = sharedSenderThreads;
  }

  synchronized Executor executor() {
    if (executor == null) {
      executor = new ThreadPoolExecutor(
          sharedSenderThreads,
          sharedSenderThreads,
          0,
          TimeUnit.MILLISECONDS,
          new SynchronousQueue<Runnable>(),
          threadFactory,
          new CallerRunsPolicy());
    } else {
      incRefCount();
    }
    return executor;
  }

  synchronized void incRefCount() {
    refCount++;
  }

  // visible for tests
  synchronized int refCount() {
    return refCount;
  }

  synchronized boolean close() {
    if (--refCount == 0 && executor != null) {
      ExecutorService executorService = (ExecutorService) this.executor;
      executorService.shutdown();
      try {
        executorService.awaitTermination(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return true;
    }
    return false;
  }
}
