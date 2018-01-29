package io.github.tramchamploo.bufferslayer;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class NoopSender<M extends Message> implements Sender<M, M> {

  private AtomicBoolean closed = new AtomicBoolean(false);

  @Override
  public List<M> send(List<M> messages) {
    if (closed.get()) {
      throw new IllegalStateException("closed");
    }
    return messages;
  }

  @Override
  public CheckResult check() {
    return CheckResult.OK;
  }

  @Override
  public void close() {
    closed.set(true);
  }
}
