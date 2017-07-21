package io.github.tramchamploo.bufferslayer;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Delegate sending and trigger onMessages afterwards
 */
public class SenderProxy implements Sender<Sql, Integer> {

  private AtomicBoolean closed = new AtomicBoolean(false);
  private Consumer<List<Integer>> onMessages = messages -> { };
  final Sender<Sql, Integer> delegate;

  public SenderProxy(Sender<Sql, Integer> delegate) {
    this.delegate = delegate;
  }

  @Override
  public CheckResult check() {
    return CheckResult.OK;
  }

  @Override
  public void close() {
    closed.set(true);
  }

  @Override
  public List<Integer> send(List<Sql> messages) {
    if (closed.get()) {
      throw new IllegalStateException("Closed!");
    }
    List<Integer> sent = delegate.send(messages);
    onMessages.accept(sent);
    return sent;
  }

  public void onMessages(Consumer<List<Integer>> onMessages) {
    this.onMessages = onMessages;
  }
}
