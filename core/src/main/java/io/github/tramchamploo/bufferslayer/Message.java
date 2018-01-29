package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.DefaultMessagePromise;
import io.github.tramchamploo.bufferslayer.internal.FailedMessageFuture;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import io.github.tramchamploo.bufferslayer.internal.MessageFuture;
import io.github.tramchamploo.bufferslayer.internal.FutureListener;
import io.github.tramchamploo.bufferslayer.internal.SucceededMessageFuture;
import java.io.Serializable;

/**
 * Message which can be sent to reporter, could be a sql statement
 */
public abstract class Message implements Serializable {

  protected static final long serialVersionUID = 845870486130911487L;

  public abstract MessageKey asMessageKey();

  /**
   * Subclasses should implement this so that callback can log this.
   */
  @Override
  public abstract String toString();

  /**
   * Return a new {@link MessagePromise}.
   */
  protected <V> MessagePromise<V> newPromise() {
    return new DefaultMessagePromise<>(this);
  }

  /**
   * Create a new {@link MessageFuture} which is marked as succeeded already. So {@link MessageFuture#isSuccess()}
   * will return {@code true}. All {@link FutureListener} added to it will be notified directly. Also
   * every call of blocking methods will just return without blocking.
   */
  protected <V> MessageFuture<V> newSucceededFuture() {
    return new SucceededMessageFuture<>(this, null);
  }

  /**
   * Create a new {@link MessageFuture} which is marked as failed already. So {@link MessageFuture#isSuccess()}
   * will return {@code false}. All {@link FutureListener} added to it will be notified directly. Also
   * every call of blocking methods will just return without blocking.
   */
  protected MessageFuture<Void> newFailedFuture(Throwable cause) {
    return new FailedMessageFuture(this, null, cause);
  }

  /**
   * If singleKey is true, we will only have one pending queue with key of this instance.
   */
  public static final MessageKey SINGLE_KEY = new MessageKey() {

    /**
     * Never expires
     */
    @Override
    long lastAccessNanos() {
      return Long.MAX_VALUE;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return this == obj;
    }

    @Override
    public String toString() {
      return "SINGLE_KEY";
    }
  };

  /**
   * Message will be put in a map with this key
   */
  public static abstract class MessageKey {

    private long lastAccessNanos = System.nanoTime();

    /**
     * set last access time to now
     */
    synchronized void recordAccess() {
      lastAccessNanos = System.nanoTime();
    }

    /**
     * last time of this key accessed in nanoseconds
     */
    synchronized long lastAccessNanos() {
      return lastAccessNanos;
    }

    /**
     * subclasses should implement this, and message will be aggregated using this
     */
    @Override
    public abstract int hashCode();

    /**
     * subclasses should implement this, and message will be aggregated using this
     */
    @Override
    public abstract boolean equals(Object obj);

    /**
     * This will be logged duration queue creation and destroy.
     */
    @Override
    public abstract String toString();
  }
}
