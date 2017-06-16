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

/**
 * Created by tramchamploo on 2017/4/14.
 */
final class SenderToAsyncSenderAdaptor<M extends Message, R> implements AsyncSender<M, R> {

  final Sender<M, R> delegate;
  final Executor executor;

  SenderToAsyncSenderAdaptor(Sender<M, R> delegate, long reporterId, int senderThreads) {
    this.delegate = checkNotNull(delegate);
    checkArgument(senderThreads > 0, "senderThreads > 0: %s", senderThreads);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("AsyncReporter-" + reporterId + "-sender-%d")
        .setDaemon(true)
        .build();
    this.executor = new ThreadPoolExecutor(senderThreads,
        senderThreads,
        0,
        TimeUnit.MILLISECONDS,
        new SynchronousQueue<Runnable>(),
        threadFactory,
        new CallerRunsPolicy());
  }

  @Override
  public Promise<List<R>, MessageDroppedException, ?> send(final List<M> messages) {
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
