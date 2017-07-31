package io.github.tramchamploo.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.github.tramchamploo.bufferslayer.internal.Deferreds;
import io.github.tramchamploo.bufferslayer.internal.SendingTask;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.multiple.MasterDeferredObject;
import org.jdeferred.multiple.MasterProgress;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneReject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert a synchronous sender to an async one by submitting tasks to an executor
 */
final class AsyncSenderAdaptor<M extends Message, R> implements AsyncSender<M, R> {

  private static final Logger logger = LoggerFactory.getLogger(AsyncSenderAdaptor.class);
  static SenderExecutorHolder executorHolder;

  private final SyncSender<M, R> delegate;
  private final Executor executor;

  AsyncSenderAdaptor(SyncSender<M, R> delegate, int sharedSenderThreads) {
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
  public Promise<MultipleResults, OneReject, MasterProgress> send(final List<SendingTask<M>> tasks) {
    logger.debug("Sending {} messages.", tasks.size());
    Object[] messageAndDeferred = SendingTask.unzipGeneric(tasks);
    @SuppressWarnings("unchecked")
    final List<M> messages = (List<M>) messageAndDeferred[0];
    @SuppressWarnings("unchecked")
    final List<Deferred> deferreds = (List<Deferred>) messageAndDeferred[1];
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          List<R> result = delegate.send(messages);
          Deferreds.resolveAll(result, deferreds);
        } catch (Throwable t) {
          Deferreds.rejectAll(t, deferreds, messages);
        }
      }
    });
    return new MasterDeferredObject(deferreds.toArray(new Deferred[0])).promise();
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
