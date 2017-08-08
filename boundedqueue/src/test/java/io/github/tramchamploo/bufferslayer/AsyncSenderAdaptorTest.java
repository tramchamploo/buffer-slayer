package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.Util.newSendingTask;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.jdeferred.Promise;
import org.jdeferred.multiple.MasterProgress;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneReject;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsyncSenderAdaptorTest {

  private AsyncSenderAdaptor<TestMessage, Integer> adaptor;

  @BeforeClass
  public static void setup() {
    if (AsyncSenderAdaptor.executorHolder != null) {
      AsyncSenderAdaptor.executorHolder = null;
    }
  }

  @After
  public void tearDown() throws IOException {
    adaptor.close();
  }

  private Object[] collectResults(MultipleResults mr) {
    int size = mr.size();
    Object[] results = new Object[size];
    for (int i = 0 ; i < size; i++) {
      results[i] = mr.get(i).getResult();
    }
    return results;
  }

  @Test
  public void sendingSuccess() throws InterruptedException {
    FakeSender sender = new FakeSender();
    CountDownLatch countDown = new CountDownLatch(1);

    adaptor = new AsyncSenderAdaptor<>(sender, 1);
    Promise<MultipleResults, OneReject, MasterProgress> promise = adaptor.send(
        Arrays.asList(newSendingTask(0), newSendingTask(1), newSendingTask(2)));
    promise.done(mr -> {
      assertArrayEquals(new Integer[]{0, 1, 2}, collectResults(mr));
      countDown.countDown();
    });
    assertTrue(countDown.await(500, TimeUnit.MILLISECONDS));
  }

  @Test
  public void sendingFailed() throws InterruptedException {
    FakeSender sender = new FakeSender();
    RuntimeException ex = new RuntimeException("expected");
    sender.onMessages(messages -> {
      throw ex;
    });
    CountDownLatch countDown = new CountDownLatch(1);

    adaptor = new AsyncSenderAdaptor<>(sender, 1);
    Promise<MultipleResults, OneReject, MasterProgress> promise = adaptor.send(
        Arrays.asList(newSendingTask(0), newSendingTask(1), newSendingTask(2)));
    promise.fail(t -> {
      assertEquals(ex, ((MessageDroppedException) t.getReject()).getCause());
      countDown.countDown();
    });
    assertTrue(countDown.await(500, TimeUnit.MILLISECONDS));
  }

  @Test
  public void callerRunsWhenThreadPoolFull() throws InterruptedException {
    CyclicBarrier barrier = new CyclicBarrier(2);
    CountDownLatch countDown = new CountDownLatch(2);

    FakeSender sender = new FakeSender();

    sender.onMessages(messages -> {
      try {
        barrier.await();
      } catch (Exception e) {
        assertFalse(true);
      }
    });

    adaptor = new AsyncSenderAdaptor<>(sender, 1);
    // block sender thread
    adaptor.send(singletonList(newSendingTask(0))).done(d -> {
      assertTrue(Thread.currentThread().getName().startsWith("AsyncReporter-sender-"));
      countDown.countDown();
    });
    // caller runs and reset barrier
    adaptor.send(singletonList(newSendingTask(0))).done(d -> {
      assertEquals("main", Thread.currentThread().getName());
      countDown.countDown();
    });

    assertTrue(countDown.await(500, TimeUnit.MILLISECONDS));
    assertEquals(0, barrier.getNumberWaiting());
  }


  @Test
  public void senderThreadName() throws InterruptedException {
    FakeSender sender = new FakeSender();

    sender.onMessages(messages ->
        assertTrue(Thread.currentThread().getName().startsWith("AsyncReporter-sender-")));

    CountDownLatch countDown = new CountDownLatch(1);

    adaptor = new AsyncSenderAdaptor<>(sender, 1);
    adaptor.send(singletonList(newSendingTask(0))).done(i -> countDown.countDown());
    assertTrue(countDown.await(500, TimeUnit.MILLISECONDS));
  }

  @Test
  public void refCount() throws IOException {
    FakeSender sender = new FakeSender();
    adaptor = new AsyncSenderAdaptor<>(sender, 1);
    assertEquals(1, AsyncSenderAdaptor.executorHolder.refCount());

    AsyncSenderAdaptor adaptor2 = new AsyncSenderAdaptor<>(sender, 1);
    assertEquals(2, AsyncSenderAdaptor.executorHolder.refCount());

    adaptor2.close();
    assertEquals(1, AsyncSenderAdaptor.executorHolder.refCount());

    adaptor.close();
    assertNull(AsyncSenderAdaptor.executorHolder);
  }
}
