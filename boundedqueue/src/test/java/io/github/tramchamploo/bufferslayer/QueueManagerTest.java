package io.github.tramchamploo.bufferslayer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import io.github.tramchamploo.bufferslayer.QueueManager.CreateCallback;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;

public class QueueManagerTest {

  private QueueManager manager;
  @SuppressWarnings("unchecked")
  private TimeDriven<MessageKey> timeDriven = mock(TimeDriven.class);
  private ReporterMetrics metrics = mock(ReporterMetrics.class);

  private static MessageKey testKey() {
    return new MessageKey() {
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
  }

  @Before
  public void setup() {
    HashedWheelTimer timer = new HashedWheelTimer(Executors.defaultThreadFactory(),
        10, TimeUnit.MILLISECONDS,
        512);
    timer.start();

    manager = new QueueManager(5,
        Strategy.Fail, TimeUnit.MILLISECONDS.toNanos(50),
        timeDriven, metrics, timer);
  }
  
  @Test
  public void getAfterCreate() {
    AbstractSizeBoundedQueue q = manager.getOrCreate(testKey());
    assertEquals(q, manager.getOrCreate(testKey()));
  }

  @Test
  public void callbackTriggeredAfterCreated() throws InterruptedException {
    CountDownLatch countDown = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger();
    manager.onCreate(new CreateCallback() {
      @Override
      public void call(AbstractSizeBoundedQueue queue) {
        assertEquals(testKey(), queue.key);
        callCount.incrementAndGet();
        countDown.countDown();
      }
    });
    manager.getOrCreate(testKey());
    countDown.await();
    manager.getOrCreate(testKey());
    // only triggered once created
    assertEquals(1, callCount.get());
  }

  @Test
  public void keepAlive() throws InterruptedException {
    MessageKey key = testKey();
    AbstractSizeBoundedQueue q = manager.getOrCreate(key);
    TimeUnit.MILLISECONDS.sleep(100);

    verify(metrics).removeFromQueuedMessages(key);
    verify(timeDriven).isTimerActive(key);
    assertEquals(0, manager.keyToQueue.size());
    AbstractSizeBoundedQueue q1 = manager.getOrCreate(testKey());
    assertNotEquals(q, q1);

    TimeUnit.MILLISECONDS.sleep(20);
    manager.getOrCreate(testKey());
    assertEquals(1, manager.keyToQueue.size());
    assertEquals(q1, manager.getOrCreate(testKey()));
  }

  @Test
  public void singleKeyNeverExpire() throws InterruptedException {
    manager.getOrCreate(Message.SINGLE_KEY);
    TimeUnit.MILLISECONDS.sleep(100);

    verifyZeroInteractions(metrics);
  }
}
