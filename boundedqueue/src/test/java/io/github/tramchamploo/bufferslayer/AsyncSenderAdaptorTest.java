package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.github.tramchamploo.bufferslayer.internal.CompositeFuture;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
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

  @Test
  public void sendingSuccess() throws InterruptedException {
    FakeSender sender = new FakeSender();
    CountDownLatch countDown = new CountDownLatch(1);

    adaptor = new AsyncSenderAdaptor<>(sender, 1);
    CompositeFuture future = adaptor.send(
        Arrays.asList(newPromise(0), newPromise(1), newPromise(2)));
    future.addListener(f -> {
      assertEquals(new Integer(0), future.<Integer>resultAt(0));
      assertEquals(new Integer(1), future.<Integer>resultAt(1));
      assertEquals(new Integer(2), future.<Integer>resultAt(2));
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
    CompositeFuture future = adaptor
        .send(Arrays.asList(newPromise(0), newPromise(1), newPromise(2)));

    future.addListener(f -> {
      assertFalse(f.isSuccess());
      assertEquals(ex, future.cause().getCause());
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
    adaptor.send(Collections.singletonList(newPromise(0))).addListener(f -> {
      assertTrue(Thread.currentThread().getName().startsWith("AsyncReporter-sender-"));
      countDown.countDown();
    });
    // caller runs and reset barrier
    adaptor.send(Collections.singletonList(newPromise(0))).addListener(f -> {
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
    adaptor.send(Collections.singletonList(newPromise(0))).addListener(f -> countDown.countDown());
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

  private static MessagePromise<Integer> newPromise(int key) {
    return newMessage(key).newPromise();
  }
}
