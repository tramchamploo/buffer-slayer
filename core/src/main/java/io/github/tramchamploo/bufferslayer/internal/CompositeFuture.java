package io.github.tramchamploo.bufferslayer.internal;

import java.util.List;

/**
 * The composite future wraps a list of {@link Future futures}, it is useful when several futures
 * needs to be coordinated.
 */
public abstract class CompositeFuture extends AbstractFuture<CompositeFuture> {

  /**
   * Return a composite future, succeeded when all futures are succeeded, failed when any future is failed.
   * <p/>
   * When the list is empty, the returned future will be already completed.
   */
  public static CompositeFuture all(List<? extends Future<?>> futures) {
    return DefaultCompositeFuture.all(futures.toArray(new Future[futures.size()]));
  }

  /**
   * Returns a cause of a wrapped future
   *
   * @param index the wrapped future index
   */
  public abstract Throwable cause(int index);

  /**
   * Returns true if a wrapped future is done
   *
   * @param index the wrapped future index
   */
  public abstract boolean isDone(int index);

  /**
   * Returns true if a wrapped future is success
   *
   * @param index the wrapped future index
   */
  public abstract boolean isSuccess(int index);

  /**
   * Return result of a wrapped future
   * @param index the wrapped future index
   */
  public abstract <T> T resultAt(int index);

  @Override
  public abstract DefaultCompositeFuture addListener(GenericFutureListener<? extends Future<? super CompositeFuture>> listener);
}
