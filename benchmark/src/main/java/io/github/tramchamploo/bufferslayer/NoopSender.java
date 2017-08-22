package io.github.tramchamploo.bufferslayer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class NoopSender implements SyncSender {

  private AtomicBoolean closed = new AtomicBoolean(false);

  @Override
  public List send(List messages) {
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
  public void close() throws IOException {
    closed.set(true);
  }
}
