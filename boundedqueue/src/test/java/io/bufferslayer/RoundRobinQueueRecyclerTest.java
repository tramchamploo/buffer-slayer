package io.bufferslayer;

import static io.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.bufferslayer.OverflowStrategy.Strategy;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Created by tramchamploo on 2017/5/19.
 */
public class RoundRobinQueueRecyclerTest extends QueueRecyclerTest {

  @Override
  AbstractQueueRecycler newRecycler() {
    return new RoundRobinQueueRecycler(5, Strategy.Fail,
        TimeUnit.MILLISECONDS.toNanos(10));
  }

  @Test
  public void roundRobinLease() {
    QueueRecycler recycler = newRecycler();
    SizeBoundedQueue q0 = recycler.getOrCreate(newMessage(0).asMessageKey());
    SizeBoundedQueue q1 = recycler.getOrCreate(newMessage(1).asMessageKey());
    SizeBoundedQueue q2 = recycler.getOrCreate(newMessage(2).asMessageKey());

    assertEquals(q0, recycler.lease(1, TimeUnit.MILLISECONDS));
    assertEquals(q1, recycler.lease(1, TimeUnit.MILLISECONDS));
    assertEquals(q2, recycler.lease(1, TimeUnit.MILLISECONDS));
    assertNull(recycler.lease(1, TimeUnit.MILLISECONDS));

    recycler.recycle(q1);
    recycler.recycle(q2);
    recycler.recycle(q0);
    assertEquals(q1, recycler.lease(1, TimeUnit.MILLISECONDS));
    assertEquals(q2, recycler.lease(1, TimeUnit.MILLISECONDS));
    assertEquals(q0, recycler.lease(1, TimeUnit.MILLISECONDS));
  }
}
