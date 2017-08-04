package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.TestMessage.newMessage;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jdeferred.Promise;
import org.junit.Test;

public class AsyncSenderAdaptorTest {

  private AsyncSenderAdaptor<TestMessage, Integer> adaptor;

  @Test
  public void sendingSuccess() throws InterruptedException {
    FakeSender sender = new FakeSender();
    CountDownLatch countDown = new CountDownLatch(1);

    adaptor = new AsyncSenderAdaptor<>(sender, 1);
    Promise<List<Integer>, MessageDroppedException, ?> promise = adaptor.send(
        Arrays.asList(newMessage(0), newMessage(1), newMessage(2)));
    promise.done(d -> {
      assertArrayEquals(new Integer[]{0, 1, 2}, d.toArray());
      countDown.countDown();
    });
    countDown.await(500, TimeUnit.MILLISECONDS);
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
    Promise<List<Integer>, MessageDroppedException, ?> promise = adaptor.send(
        Arrays.asList(newMessage(0), newMessage(1), newMessage(2)));
    promise.fail(t -> {
      assertEquals(ex, t.getCause());
      countDown.countDown();
    });
    countDown.await(500, TimeUnit.MILLISECONDS);
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
    adaptor.send(singletonList(newMessage(0))).done(d -> {
      assertEquals("AsyncReporter-sender-0", Thread.currentThread().getName());
      countDown.countDown();
    });
    // caller runs and reset barrier
    adaptor.send(singletonList(newMessage(0))).done(d -> {
      assertEquals("main", Thread.currentThread().getName());
      countDown.countDown();
    });

    countDown.await(500, TimeUnit.MILLISECONDS);
    assertEquals(0, barrier.getNumberWaiting());
  }

  @Test
  public void senderThreadName() throws InterruptedException {
    FakeSender sender = new FakeSender();

    AtomicInteger senderId = new AtomicInteger(0);
    sender.onMessages(messages -> {
      String threadName = Thread.currentThread().getName();
      assertEquals("AsyncReporter-sender-" + senderId.get(), threadName);
    });

    CountDownLatch countDown = new CountDownLatch(1);

    adaptor = new AsyncSenderAdaptor<>(sender, 1);
    adaptor.send(singletonList(newMessage(0))).then(i -> {
      countDown.countDown();
    });
    countDown.await(500, TimeUnit.MILLISECONDS);
  }
}
