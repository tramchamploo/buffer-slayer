package io.github.tramchamploo.bufferslayer;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@SuppressWarnings("unchecked")
public class RxReporterBenchmark extends AbstractReporterBenchmark {

  @Override
  protected Reporter getReporter() {
    return RxReporter.builder(sender)
        .pendingMaxMessages(1000000)
        .metrics(metrics)
        .messageTimeout(1000, TimeUnit.NANOSECONDS)
        .build();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + RxReporterBenchmark.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
