package io.github.tramchamploo.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert a synchronous sender to an async one by submitting tasks to an executor
 */
final class AsyncSenderAdaptor<M extends Message, R> implements AsyncSender<M, R> {

  private static final Logger logger = LoggerFactory.getLogger(AsyncReporter.class);
  private static SenderExecutorHolder executorHolder;

  private final Sender<M, R> delegate;
  private final Executor executor;

  AsyncSenderAdaptor(Sender<M, R> delegate, int sharedSenderThreads) {
    this.delegate = checkNotNull(delegate);
    checkArgument(sharedSenderThreads > 0, "sharedSenderThreads > 0: %s", sharedSenderThreads);
    synchronized (AsyncSenderAdaptor.class) {
      if (executorHolder == null) {
        executorHolder = new SenderExecutorHolder(sharedSenderThreads);
      }
    }
    this.executor = executorHolder.executor();
  }

  @Override
  public Promise<List<R>, MessageDroppedException, ?> send(final List<M> messages) {
    logger.debug("Sending {} messages.", messages.size());
    final Deferred<List<R>, MessageDroppedException, ?> result = new DeferredObject<>();
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          result.resolve(delegate.send(messages));
        } catch (Throwable t) {
          result.reject(MessageDroppedException.dropped(t, messages));
        }
      }
    });
    return result.promise();
  }

  @Override
  public CheckResult check() {
    return delegate.check();
  }

  @Override
  public void close() throws IOException {
    if (executor instanceof ExecutorService) {
      ((ExecutorService) executor).shutdown();
    }
    delegate.close();
  }
}
