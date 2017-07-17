package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Test;

public class RxReporterTest {

  private Reporter<TestMessage, Integer> reporter;
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

    reporter = RxReporter.builder(sender)
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

    reporter = RxReporter.builder(sender)
        .pendingMaxMessages(10)
        .bufferedMaxMessages(2)
        .messageTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
        .build();

    for (int j = 0; j < 10; j++) {
      for (int i = 0; i < 5; i++) {
        reporter.report(newMessage(i));
      }
    }

    assertTrue(countDown.await(200, TimeUnit.MILLISECONDS));
    assertEquals(50, sender.sent.size());
  }

  @Test
  public void dropWhenExceedsMaxPending() throws InterruptedException {
    FakeSender sender = new FakeSender();

    sender.onMessages(m -> safeSleep(100));

    reporter = RxReporter.builder(sender)
        .metrics(metrics)
        .pendingMaxMessages(1)
        .bufferedMaxMessages(1)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();
    for (int i = 0; i < 500; i++) {
      reporter.report(newMessage(0));
    }

    assertTrue(metrics.messagesDropped() > 0);
  }

  @Test
  public void incrementMetricsAfterReport() throws InterruptedException {
    FakeSender sender = new FakeSender();
    CountDownLatch countDown = new CountDownLatch(1);
    sender.onMessages(messages -> countDown.countDown());

    reporter = RxReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(100, TimeUnit.MILLISECONDS)
        .build();

    reporter.report(newMessage(0));
    reporter.report(newMessage(0));
    countDown.await();

    assertEquals(0, metrics.messagesDropped());
    assertEquals(2, metrics.messages());
  }

  @Test
  public void autoFlushWhenClose() throws Exception {
    FakeSender sender = new FakeSender();

    reporter = RxReporter.builder(sender)
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

    reporter = RxReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(1, TimeUnit.MILLISECONDS)
        .build();

    sender.close();
    reporter.report(newMessage(0));

    safeSleep(50);
    assertEquals(1, metrics.messagesDropped());
  }

  @Test
  public void dropWhenReporterClosed() throws IOException {
    FakeSender sender = new FakeSender();

    reporter = RxReporter.builder(sender)
        .metrics(metrics)
        .messageTimeout(1, TimeUnit.MILLISECONDS)
        .build();

    reporter.close();
    reporter.report(newMessage(0));

    assertEquals(1, metrics.messagesDropped());
  }

  private static void safeSleep(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignored) {
    }
  }
}
