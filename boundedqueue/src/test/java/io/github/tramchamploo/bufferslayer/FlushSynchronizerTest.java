package io.github.tramchamploo.bufferslayer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

public class FlushSynchronizerTest {

  private FlushSynchronizer synchronizer = new FlushSynchronizer();

  @After
  public void cleanup() {
    synchronizer.clear();
  }

  @Test
  public void oneQueueShouldNotBeOfferedTwice() {
    BlockingSizeBoundedQueue q = new BlockingSizeBoundedQueue(1, Strategy.DropTail);
    assertTrue(synchronizer.offer(q));
    assertFalse(synchronizer.offer(q));
  }

  @Test
  public void pollShouldBlockUtilQueueOccur() throws InterruptedException {
    CountDownLatch countDown = new CountDownLatch(1);

    new Thread(() -> {
      AbstractSizeBoundedQueue q = synchronizer.poll(Long.MAX_VALUE);
      if (q != null) countDown.countDown();
    }).start();

    new Thread(() -> {
      try {
        Thread.sleep(50);
        synchronizer.offer(new BlockingSizeBoundedQueue(1, Strategy.DropTail));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }).start();

    assertFalse(countDown.await(40, TimeUnit.MILLISECONDS));
    assertTrue(countDown.await(20, TimeUnit.MILLISECONDS));
  }

  @Test
  public void returnNullIfExceedsTimeout() throws InterruptedException {
    CountDownLatch countDown = new CountDownLatch(1);

    new Thread(() -> {
      assertNull(synchronizer.poll(TimeUnit.MILLISECONDS.toNanos(50)));
      countDown.countDown();
    }).start();

    assertFalse(countDown.await(40, TimeUnit.MILLISECONDS));
    assertTrue(countDown.await(40, TimeUnit.MILLISECONDS));
  }
}
