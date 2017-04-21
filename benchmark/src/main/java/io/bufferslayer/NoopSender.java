package io.bufferslayer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by guohang.bao on 2017/3/27.
 */
public class NoopSender implements Sender {

  AtomicBoolean closed = new AtomicBoolean(false);

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
