package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.jdeferred.Promise;
import org.junit.After;
import org.junit.Test;

public class AsyncReporterTest {

  private AsyncReporter<TestMessage, Integer> reporter;
  private InMemoryReporterMetrics metrics =
      InMemoryReporterMetrics.instance(ReporterMetricsExporter.NOOP_EXPORTER);

  @After
  public void close() {
    metrics.clear();
  }

  @Test
  public void flushIfExceedTimeout() throws InterruptedException {
    FakeSender sender = new FakeSender();
    CountDownLatch countDown = new CountDownLatch(1);
    sender.onMessages(m -> countDown.countDown());

    reporter = AsyncReporter.builder(sender)
        .pendingMaxMessages(2)
        .bufferedMaxMessages(2)
        .messageTimeout(10, TimeUnit.MILLISECONDS)
        .build();
    reporter.report(newMessage(0));

    assertFalse(countDown.await(5, TimeUnit.MILLISECONDS));
    assertTrue(countDown.await(30, TimeUnit.MILLISECONDS));
    assertEquals(0, sender.sent.get(0).key);
  }

  @Test
  public void flushIfExceedMaxSize() throws InterruptedException {
    FakeSender sender = new FakeSender();

    CountDownLatch countDown = new CountDownLatch(5 * 10);
    sender.onMessages(m -> IntStream.range(0, m.size()).forEach(i -> countDown.countDown()));

    reporter = AsyncReporter.builder(sender)
        .pendingMaxMessages(10)
        .bufferedMaxMessages(2)
        .messageTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
        .flushThreads(5)
        .build();

    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 5; i++) {
        reporter.report(newMessage(i));
      }
    }

    assertTrue(countDown.await(200, TimeUnit.MILLISECONDS));
    assertEquals(50, sender.sent.size());

    // make sure the queue is released
    assertEquals(0, reporter.synchronizer.deque.size());
    assertEquals(0, reporter.synchronizer.keyToReady.size());
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

    assertEquals(2, reporter.queueManager.elements().size());
  }

  @Test
  public void singleKey_differentMessagesShareSamePendingQueue() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .pendingKeepalive(1, TimeUnit.SECONDS)
        .singleKey(true)
        .build();
    reporter.report(newMessage(0));
    reporter.report(newMessage(1));

    assertEquals(1, reporter.queueManager.elements().size());
  }

  @Test
  public void sameMessagesHaveShareSamePendingQueue() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .pendingKeepalive(1, TimeUnit.SECONDS)
        .build();
    reporter.report(newMessage(0));
    reporter.report(newMessage(0));

    assertEquals(1, reporter.queueManager.elements().size());
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
  public void dropWhenReporterClosed() throws IOException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(1, TimeUnit.MILLISECONDS)
        .build();

    reporter.close();
    reporter.report(newMessage(0));

    assertEquals(1, metrics.messagesDropped());
  }

  @Test
  public void flushThreadName() {
    FakeSender sender = new FakeSender();

    AsyncReporter.idGenerator = new AtomicLong(0);
    reporter = AsyncReporter.builder(sender).build();
    reporter.report(newMessage(0));

    Iterator<Thread> iter = reporter.flushers.iterator();
    assertTrue(iter.hasNext());
    assertEquals("AsyncReporter-0-flusher-0", iter.next().getName());

    reporter = AsyncReporter.builder(sender).build();
    reporter.report(newMessage(0));

    iter = reporter.flushers.iterator();
    assertTrue(iter.hasNext());
    assertEquals("AsyncReporter-1-flusher-0", iter.next().getName());
  }

  @Test
  public void closeShouldRemoveMetrics() throws InterruptedException {
    FakeSender sender = new FakeSender();

    reporter = AsyncReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    CountDownLatch countDown = new CountDownLatch(1);
    Promise<Integer, MessageDroppedException, Integer> promise = reporter.report(
        newMessage(0));
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

  @Test
  public void lazyInitFlushThreads() throws InterruptedException {
    FakeSender sender = new FakeSender();
    reporter = AsyncReporter.builder(sender).flushThreads(1).build();

    assertEquals(0, reporter.flushers.size());
    reporter.report(newMessage(0));
    assertEquals(1, reporter.flushers.size());
    reporter.report(newMessage(0));
    assertEquals(1, reporter.flushers.size());
  }
}
