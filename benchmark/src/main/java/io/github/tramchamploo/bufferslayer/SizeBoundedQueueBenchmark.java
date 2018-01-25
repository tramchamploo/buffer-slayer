package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.MessageFuture;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
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
public class SizeBoundedQueueBenchmark {

  @State(Scope.Thread)
  public static class Factory {
    MessagePromise<Object> promise = null;

    @Setup(Level.Invocation)
    public void setup() {
      promise = TestMessage.newMessage(0).newPromise();
    }
  }

  @AuxCounters
  @State(Scope.Thread)
  public static class OfferCounters {
    public int offersMade;
    public int offersFailed;

    void addCallback(MessageFuture<?> future) {
      future.addListener(f -> {
        if (f.isSuccess()) {
          offersMade += 1;
        } else {
          offersFailed += 1;
        }
      });
    }

    @Setup(Level.Iteration)
    public void clean() {
      offersFailed = offersMade = 0;
    }
  }

  @AuxCounters
  @State(Scope.Thread)
  public static class DrainCounters {
    public int drained;

    @Setup(Level.Iteration)
    public void clean() {
      drained = 0;
    }
  }

  private static ThreadLocal<Object> marker = new ThreadLocal<>();

  @State(Scope.Thread)
  public static class ConsumerMarker {
    public ConsumerMarker() {
      marker.set(this);
    }
  }

  SizeBoundedQueue q;

  @Setup
  public void setup() {
    q = new SizeBoundedQueue(10000, OverflowStrategy.block, Message.SINGLE_KEY);
    q._setBenchmarkMode(true);
  }

  @TearDown(Level.Iteration)
  public void emptyQ() {
    // If this thread didn't drain, return
    if (marker.get() == null) return;
    q.clear();
  }

  @Benchmark @Group("no_contention") @GroupThreads(1)
  public void no_contention_offer(OfferCounters counters, Factory factory) {
    counters.addCallback(factory.promise);
    q.offer(factory.promise);
  }

  @Benchmark @Group("no_contention") @GroupThreads(1)
  public void no_contention_drain(DrainCounters counters, ConsumerMarker cm) {
    q.drainTo(next -> {
      counters.drained++;
      return true;
    });
  }

  @Benchmark @Group("mild_contention") @GroupThreads(2)
  public void mild_contention_offer(OfferCounters counters, Factory factory) {
    counters.addCallback(factory.promise);
    q.offer(factory.promise);
  }

  @Benchmark @Group("mild_contention") @GroupThreads(1)
  public void mild_contention_drain(DrainCounters counters, ConsumerMarker cm) {
    q.drainTo(next -> {
      counters.drained++;
      return true;
    });
  }

  @Benchmark @Group("high_contention") @GroupThreads(8)
  public void high_contention_offer(OfferCounters counters, Factory factory) {
    counters.addCallback(factory.promise);
    q.offer(factory.promise);
  }

  @Benchmark @Group("high_contention") @GroupThreads(1)
  public void high_contention_drain(DrainCounters counters, ConsumerMarker cm) {
    q.drainTo(next -> {
      counters.drained++;
      return true;
    });
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + SizeBoundedQueueBenchmark.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
