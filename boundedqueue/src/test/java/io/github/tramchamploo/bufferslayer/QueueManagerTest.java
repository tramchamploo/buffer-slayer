package io.github.tramchamploo.bufferslayer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import io.github.tramchamploo.bufferslayer.QueueManager.Callback;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class QueueManagerTest {

  QueueManager manager;

  static MessageKey testKey = new MessageKey() {
    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return true;
    }

    @Override
    public String toString() {
      return "testKey";
    }
  };

  @Before
  public void setup() {
    manager = new QueueManager(5, Strategy.Fail,
        TimeUnit.MILLISECONDS.toNanos(10));
  }
  
  @Test
  public void getAfterCreate() {
    SizeBoundedQueue q = manager.getOrCreate(testKey);
    assertEquals(q, manager.getOrCreate(testKey));
  }

  @Test
  public void callbackTriggeredAfterCreated() throws InterruptedException {
    CountDownLatch countDown = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger();
    manager.onCreate(new Callback() {
      @Override
      public void call(SizeBoundedQueue queue) {
        assertEquals(testKey, queue.key);
        callCount.incrementAndGet();
        countDown.countDown();
      }
    });
    manager.getOrCreate(testKey);
    countDown.await();
    manager.getOrCreate(testKey);
    // only triggered once created
    assertEquals(1, callCount.get());
  }

  @Test
  public void keepAlive() throws InterruptedException {
    SizeBoundedQueue q = manager.getOrCreate(testKey);
    TimeUnit.MILLISECONDS.sleep(10);

    assertEquals(testKey, manager.shrink().get(0));
    assertEquals(0, manager.keyToQueue.size());
    SizeBoundedQueue q1 = manager.getOrCreate(testKey);
    assertNotEquals(q, q1);

    TimeUnit.MILLISECONDS.sleep(10);
    manager.getOrCreate(testKey);
    assertEquals(1, manager.keyToQueue.size());
    assertEquals(q1, manager.getOrCreate(testKey));
  }

  @Test
  public void singleKeyNeverExpire() throws InterruptedException {
    manager.getOrCreate(Message.SINGLE_KEY);
    TimeUnit.MILLISECONDS.sleep(10);

    assertEquals(0, manager.shrink().size());
  }
}
