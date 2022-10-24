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
@Warmup(iterations = 3, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Group)
public class AsyncBatchJedisBenchmark extends AbstractBatchJedisBenchmark {

  protected Reporter<RedisCommand, Object> reporter(Sender<RedisCommand, Object> sender) {
    return AsyncReporter.builder(sender)
        .pendingKeepalive(1, TimeUnit.SECONDS)
        .build();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + AsyncBatchJedisBenchmark.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
