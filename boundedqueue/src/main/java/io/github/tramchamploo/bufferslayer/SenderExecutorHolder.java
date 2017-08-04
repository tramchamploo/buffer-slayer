package io.github.tramchamploo.bufferslayer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executor;
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
      executor = new ThreadPoolExecutor(sharedSenderThreads,
          sharedSenderThreads,
          0,
          TimeUnit.MILLISECONDS,
          new SynchronousQueue<Runnable>(),
          threadFactory,
          new CallerRunsPolicy());
    }
    return executor;
  }
}
