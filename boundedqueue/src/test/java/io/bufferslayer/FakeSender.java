package io.bufferslayer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Created by guohang.bao on 2017/3/14.
 */
public class FakeSender implements Sender<TestMessage, Integer> {

  private AtomicBoolean closed = new AtomicBoolean(false);
  final ArrayList<TestMessage> sent = new ArrayList<>();
  private Consumer<List<TestMessage>> onMessages = messages -> {};

  @Override
  public CheckResult check() {
    return CheckResult.OK;
  }

  @Override
  public void close() {
    closed.set(true);
  }

  public void onMessages(Consumer<List<TestMessage>> onMessages) {
    this.onMessages = onMessages;
  }

  @Override
  public List<Integer> send(List<TestMessage> messages) {
    if (closed.get()) {
      throw new IllegalStateException("Closed!");
    }
    sent.addAll(messages);
    onMessages.accept(messages);
    Integer[] ret = new Integer[messages.size()];
    for (int i = 0; i < messages.size(); i++) {
      ret[i] = messages.get(i).key;
    }
    return Arrays.asList(ret);
  }
}
