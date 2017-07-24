package io.github.tramchamploo.bufferslayer;

import java.util.concurrent.TimeUnit;

public class RxReporterTimeUsedComparison extends AbstractTimeUsedComparison {

  public static void main(String[] args) throws Exception {
    new RxReporterTimeUsedComparison().run();
  }

  @Override
  protected Reporter<Sql, Integer> reporter(Sender<Sql, Integer> actual) {
    return RxReporter.builder(actual)
        .pendingMaxMessages(6000)
        .bufferedMaxMessages(100)
        .messageTimeout(50, TimeUnit.MILLISECONDS)
        .build();
  }
}
