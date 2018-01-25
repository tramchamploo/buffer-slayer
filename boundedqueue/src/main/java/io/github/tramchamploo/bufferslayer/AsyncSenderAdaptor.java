package io.github.tramchamploo.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.github.tramchamploo.bufferslayer.internal.CompositeFuture;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import io.github.tramchamploo.bufferslayer.internal.Promises;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert a synchronous sender to an async one by submitting tasks to an executor
 */
final class AsyncSenderAdaptor<M extends Message, R> implements AsyncSender<R> {

  private static final Logger logger = LoggerFactory.getLogger(AsyncReporter.class);
  static SenderExecutorHolder executorHolder;

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
  public CompositeFuture send(final List<MessagePromise<R>> promises) {
    logger.debug("Sending {} messages.", promises.size());
    final List<M> messages = extractMessages(promises);

    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          List<R> result = delegate.send(messages);
          Promises.allSuccess(result, promises);
        } catch (Throwable t) {
          Promises.allFail(t, promises, messages);
        }
      }
    });
    return CompositeFuture.all(promises);
  }

  @SuppressWarnings("unchecked")
  private List<M> extractMessages(List<MessagePromise<R>> promises) {
    List<M> messages = new ArrayList<>();
    for (MessagePromise<R> promise : promises) {
      messages.add((M) promise.message());
    }
    return messages;
  }

  @Override
  public CheckResult check() {
    return delegate.check();
  }

  @Override
  public void close() throws IOException {
    synchronized (AsyncSenderAdaptor.class) {
      if (executorHolder != null && executorHolder.close()) {
        executorHolder = null;
      }
    }
    delegate.close();
  }
}
