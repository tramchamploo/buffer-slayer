package io.github.tramchamploo.bufferslayer.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCompositeFuture extends CompositeFuture {

  @SuppressWarnings("unchecked")
  public static CompositeFuture all(Future<?>... futures) {
    final DefaultCompositeFuture composite = new DefaultCompositeFuture(futures);
    final int len = futures.length;

    if (len == 0) {
      synchronized (composite) {
        composite.done = true;
      }
    }

    for (int i = 0; i < len; i++) {
      futures[i].addListener(new FutureListener() {

        @Override
        public void operationComplete(Future future) {
          if (future.isSuccess()) {
            synchronized (composite) {
              composite.count++;
              if (!composite.isDone() && composite.count == len) {
                composite.setDone(null);
              }
            }
          } else {
            synchronized (composite) {
              composite.setDone(future.cause());
            }
          }
        }
      });
    }

    return composite;
  }

  private static final Logger logger = LoggerFactory.getLogger(DefaultCompositeFuture.class);

  private final Future[] futures;
  private int count;
  private boolean done;
  private Throwable cause;
  private Object listeners;
  private boolean notifyingListeners;

  private DefaultCompositeFuture(Future<?>... futures) {
    this.futures = futures;
  }

  @Override
  public boolean isSuccess() {
    return done && cause == null;
  }

  @Override
  public boolean isCancellable() {
    return false;
  }

  @Override
  public Throwable cause() {
    return cause;
  }

  @Override
  public DefaultCompositeFuture addListener(
      GenericFutureListener<? extends Future<? super CompositeFuture>> listener) {
    checkNotNull(listener, "listener");

    synchronized (this) {
      if (listeners == null) {
        listeners = listener;
      } else if (listeners instanceof DefaultFutureListeners) {
        ((DefaultFutureListeners) listeners).add(listener);
      } else {
        listeners = new DefaultFutureListeners((GenericFutureListener<?>) listeners, listener);
      }
    }

    if (isDone()) {
      notifyListenersNow();
    }

    return this;
  }


  @Override
  public DefaultCompositeFuture addListeners(
      GenericFutureListener<? extends Future<? super CompositeFuture>>... listeners) {
    throw new UnsupportedOperationException();
  }

  private void notifyListenersNow() {
    Object listeners;
    synchronized (this) {
      // Only proceed if there are listeners to notify and we are not already notifying listeners.
      if (notifyingListeners || this.listeners == null) {
        return;
      }
      notifyingListeners = true;
      listeners = this.listeners;
      this.listeners = null;
    }
    for (;;) {
      if (listeners instanceof DefaultFutureListeners) {
        notifyListeners0((DefaultFutureListeners) listeners);
      } else {
        notifyListener0(this, (GenericFutureListener<?>) listeners);
      }
      synchronized (this) {
        if (this.listeners == null) {
          // Nothing can throw from within this method, so setting notifyingListeners back to false does not
          // need to be in a finally block.
          notifyingListeners = false;
          return;
        }
        listeners = this.listeners;
        this.listeners = null;
      }
    }
  }

  private void notifyListeners0(DefaultFutureListeners listeners) {
    GenericFutureListener<?>[] a = listeners.listeners();
    int size = listeners.size();
    for (int i = 0; i < size; i ++) {
      notifyListener0(this, a[i]);
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static void notifyListener0(Future future, GenericFutureListener l) {
    try {
      l.operationComplete(future);
    } catch (Throwable t) {
      logger.warn("An exception was thrown by " + l.getClass().getName() + ".operationComplete()", t);
    }
  }

  @Override
  public DefaultCompositeFuture removeListener(
      GenericFutureListener<? extends Future<? super CompositeFuture>> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DefaultCompositeFuture removeListeners(
      GenericFutureListener<? extends Future<? super CompositeFuture>>... listeners) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DefaultCompositeFuture sync() throws InterruptedException {
    return this;
  }

  @Override
  public DefaultCompositeFuture syncUninterruptibly() {
    return this;
  }

  @Override
  public DefaultCompositeFuture await() throws InterruptedException {
    return this;
  }

  @Override
  public DefaultCompositeFuture awaitUninterruptibly() {
    if (isDone()) {
      return this;
    }

    boolean interrupted = false;
    synchronized (this) {
      while (!isDone()) {
        try {
          wait();
        } catch (InterruptedException e) {
          // Interrupted while waiting.
          interrupted = true;
        }
      }
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }

    return this;
  }

  @Override
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    return false;
  }

  @Override
  public boolean await(long timeoutMillis) throws InterruptedException {
    return false;
  }

  @Override
  public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
    return false;
  }

  @Override
  public boolean awaitUninterruptibly(long timeoutMillis) {
    return false;
  }

  @Override
  public CompositeFuture getNow() {
    return this;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  void setDone(Throwable cause) {
    synchronized (this) {
      if (done) {
        return;
      }
      done = true;
      this.cause = cause;
      notifyAll();
      notifyListenersNow();
    }
  }

  @Override
  public boolean isDone() {
    return done;
  }

  @Override
  public Throwable cause(int index) {
    return future(index).cause();
  }

  @Override
  public boolean isDone(int index) {
    return future(index).isDone();
  }

  @Override
  public boolean isSuccess(int index) {
    return future(index).isSuccess();
  }

  @Override
  public <T> T resultAt(int index) {
    return this.<T>future(index).getNow();
  }

  @SuppressWarnings("unchecked")
  private <T> Future<T> future(int index) {
    if (index < 0 || index > futures.length) {
      throw new IndexOutOfBoundsException();
    }
    return (Future<T>) futures[index];
  }
}
