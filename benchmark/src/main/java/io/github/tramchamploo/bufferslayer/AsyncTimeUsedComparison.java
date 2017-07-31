package io.github.tramchamploo.bufferslayer;

import java.util.concurrent.TimeUnit;

public class AsyncTimeUsedComparison extends AbstractTimeUsedComparison {

  public static void main(String[] args) throws Exception {
    new AsyncTimeUsedComparison().run();
  }

  @Override
  protected <S extends Message, R> Reporter<S, R> reporter(Sender<S, R> actual) {
    return AsyncReporter.builder(actual)
        .pendingMaxMessages(6000)
        .bufferedMaxMessages(100)
        .messageTimeout(50, TimeUnit.MILLISECONDS)
        .pendingKeepalive(10, TimeUnit.MILLISECONDS)
        .sharedSenderThreads(10)
        .singleKey(true)
        .build();
  }
}
