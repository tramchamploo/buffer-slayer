package io.github.tramchamploo.bufferslayer;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
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

  FiberSenderAdaptor(Sender<M, R> delegate) {
    this.delegate = delegate;
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
    Fiber<List<R>> fiber = new Fiber<List<R>>() {
      @Override
      protected List<R> run() throws SuspendExecution, InterruptedException {
        logger.debug("Sending {} messages.", messages.size());
        try {
          List<R> result = delegate.send(messages);
          deferred.resolve(result);
          return result;
        } catch (Throwable t) {
          deferred.reject(MessageDroppedException.dropped(t, messages));
        }
        return Collections.emptyList();
      }
    };
    fiber.start();
    return deferred.promise();
  }
}
