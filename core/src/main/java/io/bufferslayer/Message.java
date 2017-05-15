package io.bufferslayer;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by guohang.bao on 2017/3/3.
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
   * If strict order is true, we will only have one pending queue, whose key is this.
   */
  public static final MessageKey STRICT_ORDER = new MessageKey() {
    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return this == obj;
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
  }
}
