package io.bufferslayer;

import static io.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jdeferred.Promise;
import org.junit.After;
import org.junit.Test;

/**
 * Created by guohang.bao on 2017/3/14.
 */
public class AsyncReporterTest {

  private AsyncReporter reporter;
  private InMemoryReporterMetrics metrics =
      InMemoryReporterMetrics.instance(ReporterMetricsExporter.NOOP_EXPORTER);

  @After
  public void close() {
    metrics.clear();
  }

  @Test
  public void reportIfExceedTimeout() throws InterruptedException {
    FakeSender sender = new FakeSender();
    CountDownLatch countDown = new CountDownLatch(1);
    sender.onMessages(m -> countDown.countDown());

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .pendingMaxMessages(10)
        .bufferedMaxMessages(10)
        .messageTimeout(100, TimeUnit.MILLISECONDS)
        .build();
    reporter.report(newMessage(0));

    assertFalse(countDown.await(50, TimeUnit.MILLISECONDS));
    assertTrue(countDown.await(60, TimeUnit.MILLISECONDS));
    assertEquals(0, sender.sent.get(0).key);
  }

  @Test
  public void reportIfExceedMaxSize() throws InterruptedException {
    FakeSender sender = new FakeSender();
    CountDownLatch countDown = new CountDownLatch(1);
    sender.onMessages(m -> countDown.countDown());

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .pendingMaxMessages(1)
        .bufferedMaxMessages(1)
        .messageTimeout(Integer.MAX_VALUE, TimeUnit.SECONDS)
        .build();
    reporter.report(newMessage(0));
    countDown.await(10, TimeUnit.MILLISECONDS);
    assertEquals(0, sender.sent.get(0).key);
  }

  @Test
  public void differentMessagesHaveDifferentPendingQueue() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .pendingKeepalive(1, TimeUnit.SECONDS)
        .build();
    reporter.report(newMessage(0));
    reporter.report(newMessage(1));

    assertEquals(2, reporter.pendingRecycler.elements().size());
  }

  @Test
  public void strictOrdered_differentMessagesShareTheSameFlushThread() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .pendingKeepalive(1, TimeUnit.SECONDS)
        .strictOrder(true)
        .build();
    reporter.report(newMessage(0));
    reporter.report(newMessage(1));

    assertEquals(1, reporter.pendingRecycler.elements().size());
  }

  @Test
  public void sameMessagesHaveShareTheSameFlushThread() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .pendingKeepalive(1, TimeUnit.SECONDS)
        .build();
    reporter.report(newMessage(0));
    reporter.report(newMessage(0));

    assertEquals(1, reporter.pendingRecycler.elements().size());
  }

  @Test
  public void dropWhenExceedsMaxPending() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .pendingMaxMessages(1)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .pendingKeepalive(1, TimeUnit.SECONDS)
        .build();
    reporter.report(newMessage(0));
    reporter.report(newMessage(0));
    reporter.flush();
    assertEquals(1, sender.sent.size());
    assertEquals(1, metrics.messagesDropped());
  }

  @Test
  public void incrementMetricsAfterReport() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .pendingKeepalive(1, TimeUnit.SECONDS)
        .build();
    reporter.report(newMessage(0));
    reporter.report(newMessage(0));
    reporter.flush();
    assertEquals(0, metrics.messagesDropped());
    assertEquals(2, metrics.messages());
  }

  @Test
  public void flushIncrementMetrics() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .bufferedMaxMessages(1)
        .build();

    reporter.report(newMessage(0));
    reporter.report(newMessage(0));

    reporter.flush();
    assertEquals(1, metrics.queuedMessages());

    reporter.flush();
    assertEquals(0, metrics.queuedMessages());
  }

  @Test
  public void waitWhenClose() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(2, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(newMessage(0));
    reporter.close();
    assertTrue(reporter.close.await(5, TimeUnit.MILLISECONDS));
  }

  @Test
  public void autoFlushWhenClose() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(1, TimeUnit.SECONDS)
        .build();

    reporter.report(newMessage(0));
    reporter.close();
    Thread.sleep(50);
    assertEquals(1, sender.sent.size());
  }

  @Test
  public void dropWhenSenderClosed() {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(newMessage(0));
    sender.close();
    reporter.flush();

    assertEquals(1, metrics.messagesDropped());
  }

  @Test
  public void successAndFailInSameBatch() {
    FakeSender sender = new FakeSender();
    AtomicInteger count = new AtomicInteger();
    String errorMsg = "Second messages fails";

    sender.onMessages(msgs -> {
      if (count.incrementAndGet() == 2 && msgs.size() == 2) {
        throw new RuntimeException(errorMsg);
      }
    });

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .parallelismPerBatch(2)
        .senderExecutor(Executors.newCachedThreadPool())
        .build();

    Promise<Object, MessageDroppedException, Integer> promise = reporter.report(newMessage(0));
    promise.fail(ex -> {
      assertEquals(2, ex.dropped.size());
      assertEquals(errorMsg, ex.getCause().getMessage());
    });
    reporter.report(newMessage(0));

    reporter.report(newMessage(0));
    reporter.report(newMessage(0));

    reporter.flush();

    assertEquals(4, metrics.messagesDropped());
    assertEquals(4, metrics.messages());
  }

  @Test
  public void successAndFailInDifferentBatch() {
    FakeSender sender = new FakeSender();
    AtomicInteger count = new AtomicInteger();
    sender.onMessages(msgs -> {
      if (count.incrementAndGet() == 2) throw new RuntimeException("Second message fails");
    });

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .parallelismPerBatch(2)
        .senderExecutor(Executors.newCachedThreadPool())
        .build();

    reporter.report(newMessage(0));
    reporter.report(newMessage(0));
    reporter.flush();

    assertEquals(2, metrics.messagesDropped());
  }

  @Test
  public void flushThreadName() {
    FakeSender sender = new FakeSender();

    AsyncReporter.idGenerator = new AtomicLong(0);
    reporter = AsyncReporter.builder(sender).build();
    reporter.report(newMessage(0));

    Iterator<Thread> iter = reporter.flushThreads.iterator();
    assertTrue(iter.hasNext());
    assertEquals("AsyncReporter-0-flush-thread-0", iter.next().getName());

    reporter = AsyncReporter.builder(sender).build();
    reporter.report(newMessage(0));

    iter = reporter.flushThreads.iterator();
    assertTrue(iter.hasNext());
    assertEquals("AsyncReporter-1-flush-thread-0", iter.next().getName());
  }

  @Test
  public void closeShouldRemoveMetrics() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    CountDownLatch countDown = new CountDownLatch(1);
    Promise<Object, MessageDroppedException, Integer> promise = reporter.report(newMessage(0));
    promise.done(d -> {
      assertEquals(1, metrics.queuedMessages.size());
      countDown.countDown();
    });
    reporter.flush();
    countDown.await();

    reporter.close();
    assertEquals(0, metrics.queuedMessages.size());
  }

  @Test
  public void flushShouldClearOvertimeMetrics() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .pendingKeepalive(10, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(newMessage(0));
    reporter.flush();
    assertEquals(1, metrics.queuedMessages.size());

    Thread.sleep(10);
    reporter.flush();
    assertEquals(0, metrics.queuedMessages.size());
  }
}
