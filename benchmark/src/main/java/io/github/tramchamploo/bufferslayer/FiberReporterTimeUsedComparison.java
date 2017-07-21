package io.github.tramchamploo.bufferslayer;

import java.util.concurrent.TimeUnit;

public class FiberReporterTimeUsedComparison extends AbstractTimeUsedComparison {

  public static void main(String[] args) throws Exception {
    new FiberReporterTimeUsedComparison().run();
  }

  @Override
  protected Reporter<Sql, Integer> getReporter(Sender<Sql, Integer> actual) {
    return FiberReporter.fiberBuilder(actual)
        .pendingMaxMessages(6000)
        .bufferedMaxMessages(100)
        .messageTimeout(50, TimeUnit.MILLISECONDS)
        .pendingKeepalive(10, TimeUnit.MILLISECONDS)
        .senderThreads(10)
        .singleKey(true)
        .build();
  }
}
