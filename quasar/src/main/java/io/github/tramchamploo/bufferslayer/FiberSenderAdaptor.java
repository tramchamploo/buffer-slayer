package io.github.tramchamploo.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.github.tramchamploo.bufferslayer.SenderFiberAsync.exec;

import co.paralleluniverse.common.util.CheckedCallable;
import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.SuspendableRunnable;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.List;
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
 * Use quasar's fiber to execute blocking send.
 * This should be a performance improvement on sending on java's threads
 */
final class FiberSenderAdaptor<M extends Message, R> implements AsyncSender<M, R> {

  static final Logger logger = LoggerFactory.getLogger(FiberSenderAdaptor.class);

  final Sender<M, R> delegate;
  final ExecutorService executor;

  FiberSenderAdaptor(Sender<M, R> delegate, long reporterId, int senderThreads) {
    this.delegate = checkNotNull(delegate);
    checkArgument(senderThreads > 0, "senderThreads > 0: %s", senderThreads);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("FiberReporter-" + reporterId + "-sender-%d")
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
  public CheckResult check() {
    return delegate.check();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public Promise<List<R>, MessageDroppedException, ?> send(final List<M> messages) {
    final Deferred<List<R>, MessageDroppedException, ?> deferred = new DeferredObject<>();

    Fiber<Void> fiber = new Fiber<>((SuspendableRunnable) () -> {
      logger.debug("Sending {} messages.", messages.size());
      try {
        List<R> result = exec(executor, (CheckedCallable<List<R>, Exception>) () -> delegate.send(messages));
        deferred.resolve(result);
      } catch (Exception e) {
        deferred.reject(MessageDroppedException.dropped(e, messages));
      }
    });

    fiber.start();
    return deferred.promise();
  }
}
