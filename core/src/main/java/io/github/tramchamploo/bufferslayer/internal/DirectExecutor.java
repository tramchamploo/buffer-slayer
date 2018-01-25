package io.github.tramchamploo.bufferslayer.internal;

import java.util.concurrent.Executor;

public enum DirectExecutor implements Executor {
  INSTANCE;

  @Override
  public void execute(Runnable command) {
    command.run();
  }

  @Override
  public String toString() {
    return "DirectExecutor";
  }
}
