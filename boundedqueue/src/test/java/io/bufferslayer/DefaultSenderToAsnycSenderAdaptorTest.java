package io.bufferslayer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by guohang.bao on 2017/4/17.
 */
public class DefaultSenderToAsnycSenderAdaptorTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  FakeSender sender = new FakeSender();
  Executor executor = Executors.newCachedThreadPool();
  DefaultSenderToAsyncSenderAdaptor<TestMessage, Integer> adaptor;

  @Test
  public void unregularParallelism() {
    adaptor = new DefaultSenderToAsyncSenderAdaptor<>(sender, executor, 5);

    List<TestMessage> messages = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      messages.add(TestMessage.newMessage(i));
    }

    List<List<TestMessage>> parts = adaptor.split(messages);
    assertEquals(5, parts.size());

    int count = 0;
    for (List<TestMessage> part : parts) {
      count += part.size();
    }
    assertEquals(8, count);

    adaptor.send(messages)
        .done(ret -> assertEquals(5, ret.size()));
  }

  @Test
  public void regularParallelism() {
    adaptor = new DefaultSenderToAsyncSenderAdaptor<>(sender, executor, 4);

    List<TestMessage> messages = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      messages.add(TestMessage.newMessage(i));
    }

    List<List<TestMessage>> parts = adaptor.split(messages);
    assertEquals(4, parts.size());

    for (List<TestMessage> part : parts) {
      assertEquals(2, part.size());
    }

    adaptor.send(messages)
        .done(ret -> assertEquals(4, ret.size()));
  }

  @Test
  public void ListSizeLessThanParallelism() {
    adaptor = new DefaultSenderToAsyncSenderAdaptor<>(sender, executor, 4);

    List<TestMessage> messages = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      messages.add(TestMessage.newMessage(i));
    }

    List<List<TestMessage>> parts = adaptor.split(messages);
    assertEquals(3, parts.size());

    for (List<TestMessage> part : parts) {
      assertEquals(1, part.size());
    }

    adaptor.send(messages)
        .done(ret -> assertEquals(3, ret.size()));
  }
}
