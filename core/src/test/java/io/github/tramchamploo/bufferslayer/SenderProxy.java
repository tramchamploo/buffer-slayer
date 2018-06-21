package io.github.tramchamploo.bufferslayer;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Delegate sending and trigger onMessages afterwards
 */
public class SenderProxy<M extends Message, R> implements Sender<M, R> {

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private Consumer<List<M>> onMessages = messages -> { };
  private final Sender<M, R> delegate;

  SenderProxy(Sender<M, R> delegate) {
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
  public List<R> send(List<M> messages) {
    if (closed.get()) {
      throw new IllegalStateException("Closed!");
    }
    List<R> sent = delegate.send(messages);
    onMessages.accept(messages);
    return sent;
  }

  public void onMessages(Consumer<List<M>> onMessages) {
    this.onMessages = onMessages;
  }
}
