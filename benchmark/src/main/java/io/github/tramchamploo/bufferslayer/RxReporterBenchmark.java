package io.github.tramchamploo.bufferslayer;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Group)
@SuppressWarnings("unchecked")
public class RxReporterBenchmark extends AbstractReporterBenchmark {

  @Override
  protected Reporter getReporter() {
    return RxReporter.builder(sender)
        .pendingMaxMessages(100)
        .metrics(metrics)
        .messageTimeout(10, TimeUnit.MILLISECONDS)
        .build();
  }

  @Override
  protected void doClear() {
    // Seems no way to clear pending messages on a RxJava's Flowable
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + RxReporterBenchmark.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
