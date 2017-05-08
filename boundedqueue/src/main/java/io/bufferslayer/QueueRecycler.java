package io.bufferslayer;

import io.bufferslayer.Message.MessageKey;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * Created by guohang.bao on 2017/5/4.
 * A recycler that manages {@link SizeBoundedQueue}'s lifecycle
 */
public interface QueueRecycler {

  /**
   * Get a queue by message key if exists otherwise create one
   *
   * @param key message key related to the queue
   * @return the queue
   */
  SizeBoundedQueue getOrCreate(MessageKey key);

  /**
   * Lease a queue due to its priority
   *
   * @return queue with the highest priority at the moment
   */
  SizeBoundedQueue lease(long timeout, TimeUnit unit);

  /**
   * Put the queue back to recycler for others to use
   *
   * @param queue queue to return back
   */
  void recycle(SizeBoundedQueue queue);

  /**
   * Drop queues which exceed its keepalive
   */
  LinkedList<SizeBoundedQueue> shrink();

  /**
   * clear everything
   */
  void clear();
}
