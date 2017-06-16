package io.github.tramchamploo.bufferslayer;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tramchamploo on 2017/3/3.
 * Message can be send to reporter, could be a sql or a hbase query
 */
public abstract class Message implements Serializable {

  protected static final long serialVersionUID = 845870486130911487L;

  /**
   * Global unique id as key of mapping from message to deferred.
   */
  private static final AtomicLong idGenerator = new AtomicLong();
  public final Long id = idGenerator.incrementAndGet();

  public abstract MessageKey asMessageKey();

  /**
   * Subclasses should implement this so that callback can log this.
   */
  @Override
  public abstract String toString();

  /**
   * If singleKey is true, we will only have one pending queue with key of this instance.
   */
  public static final MessageKey SINGLE_KEY = new MessageKey() {
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
