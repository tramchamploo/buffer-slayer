package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.TestMessage.newMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.jdeferred.Promise;
import org.junit.Test;

public class FiberSenderAdaptorTest {

  private FiberSenderAdaptor<TestMessage, Integer> adaptor;

  @Test
  public void sendingSuccess() throws InterruptedException {
    FakeSender sender = new FakeSender();
    CountDownLatch countDown = new CountDownLatch(1);

    adaptor = new FiberSenderAdaptor<>(sender);
    Promise<List<Integer>, MessageDroppedException, ?> promise =
        adaptor.send(Arrays.asList(newMessage(0), newMessage(1), newMessage(2)));
    promise.done(d -> {
      assertArrayEquals(new Integer[]{0, 1, 2}, d.toArray());
      countDown.countDown();
    });
    countDown.await();
  }

  @Test
  public void sendingFailed() throws InterruptedException {
    FakeSender sender = new FakeSender();
    RuntimeException ex = new RuntimeException("expected");
    sender.onMessages(messages -> {
      throw ex;
    });
    CountDownLatch countDown = new CountDownLatch(1);

    adaptor = new FiberSenderAdaptor<>(sender);
    Promise<List<Integer>, MessageDroppedException, ?> promise =
        adaptor.send(Arrays.asList(newMessage(0), newMessage(1), newMessage(2)));
    promise.fail(t -> {
      assertEquals(ex, t.getCause());
      countDown.countDown();
    });
    countDown.await();
  }
}
