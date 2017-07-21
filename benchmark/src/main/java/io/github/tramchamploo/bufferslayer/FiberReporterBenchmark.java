package io.github.tramchamploo.bufferslayer;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@SuppressWarnings("unchecked")
public class FiberReporterBenchmark extends AbstractReporterBenchmark {

  @Override
  protected Reporter getReporter() {
    return FiberReporter.builder(sender)
        .pendingMaxMessages(1000000)
        .metrics(metrics)
        .messageTimeout(1000, TimeUnit.NANOSECONDS)
        .build();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + FiberReporterBenchmark.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
