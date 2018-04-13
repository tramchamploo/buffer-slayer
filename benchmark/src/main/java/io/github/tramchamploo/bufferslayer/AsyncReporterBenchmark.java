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
public class AsyncReporterBenchmark extends AbstractReporterBenchmark {

  @Override
  protected Reporter<Message, ?> getReporter() {
    return AsyncReporter.builder(sender)
        .pendingMaxMessages(100)
        .totalQueuedMessages(100)
        .metrics(metrics)
        .messageTimeout(1000, TimeUnit.NANOSECONDS)
        .build();
  }

  @Override
  protected void doClear() {
    ((AsyncReporter) reporter).clearPendings();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + AsyncReporterBenchmark.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
