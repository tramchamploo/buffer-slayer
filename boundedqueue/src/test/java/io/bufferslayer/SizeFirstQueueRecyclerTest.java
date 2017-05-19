package io.bufferslayer;

import static io.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertEquals;

import io.bufferslayer.OverflowStrategy.Strategy;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Created by tramchamploo on 2017/5/19.
 */
public class SizeFirstQueueRecyclerTest extends QueueRecyclerTest {

  @Override
  AbstractQueueRecycler newRecycler() {
    return new SizeFirstQueueRecycler(5, Strategy.Fail,
        TimeUnit.MILLISECONDS.toNanos(10));
  }

  static SizeBoundedQueue newQueue(QueueRecycler recycler, int key) {
    return recycler.getOrCreate(TestMessage.newMessage(key).asMessageKey());
  }

  @Test
  public void leaseBySize() {
    QueueRecycler recycler = newRecycler();
    SizeBoundedQueue q0 = newQueue(recycler, 0); // size 0

    SizeBoundedQueue q2 = newQueue(recycler, 2); // size 2
    q2.offer(newMessage(0), newDeferred());
    q2.offer(newMessage(0), newDeferred());

    SizeBoundedQueue q1 = newQueue(recycler, 1); // size 1
    q1.offer(newMessage(0), newDeferred());

    recycler.lease(1, TimeUnit.MILLISECONDS);
    recycler.lease(1, TimeUnit.MILLISECONDS);
    recycler.lease(1, TimeUnit.MILLISECONDS);
    recycler.recycle(q0);
    recycler.recycle(q1);
    recycler.recycle(q2);

    assertEquals(q2, recycler.lease(1, TimeUnit.MILLISECONDS));
    assertEquals(q1, recycler.lease(1, TimeUnit.MILLISECONDS));
    assertEquals(q0, recycler.lease(1, TimeUnit.MILLISECONDS));
  }
}
