package io.github.tramchamploo.bufferslayer;

import java.util.concurrent.TimeUnit;

/**
 * Created by tramchamploo on 2017/3/30.
 */
public class AsyncReporterTimeUsedComparison extends AbstractTimeUsedComparison {

  public static void main(String[] args) throws Exception {
    new AsyncReporterTimeUsedComparison().run();
  }

  @Override
  protected Reporter<Sql, Integer> getReporter(Sender<Sql, Integer> actual) {
    return AsyncReporter.builder(actual)
        .pendingMaxMessages(6000)
        .bufferedMaxMessages(100)
        .messageTimeout(50, TimeUnit.MILLISECONDS)
        .pendingKeepalive(10, TimeUnit.MILLISECONDS)
        .senderThreads(10)
        .singleKey(true)
        .build();
  }
}
