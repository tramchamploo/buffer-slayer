package io.bufferslayer;

import java.util.concurrent.TimeUnit;
import org.jdeferred.Deferred;
import org.jdeferred.impl.DeferredObject;
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

/**
 * Created by guohang.bao on 2017/3/16.
 */
@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Group)
public class SizeBoundedQueueBenchmark {
  static Message message = TestMessage.newMessage(0);

  static int offersFailed;
  static int offersMade;

  static Deferred<?, MessageDroppedException, Integer> newDeferred() {
    return new DeferredObject<>();
  }

  @AuxCounters
  @State(Scope.Thread)
  public static class OfferCounters {

    Deferred<?, MessageDroppedException, Integer> deferred = newDeferred();

    public int offersMade() {
      return offersMade;
    }

    public int offersFailed() {
      return offersFailed;
    }

    @Setup(Level.Iteration)
    public void clean() {
      offersFailed = offersMade = 0;
    }

    @Setup(Level.Invocation)
    public void setup() {
      deferred = newDeferred();
      deferred.promise()
          .progress(count -> offersMade += count)
          .fail(ex -> offersFailed += 1);
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
    q = new SizeBoundedQueue(10000, OverflowStrategy.dropNew);
  }

  @TearDown(Level.Iteration)
  public void emptyQ() {
    // If this thread didn't drain, return
    if (marker.get() == null) return;
    q.clear();
  }

  @Benchmark
  @Group("no_contention") @GroupThreads(1)
  public void no_contention_offer(OfferCounters counters) {
    q.offer(message, counters.deferred);
  }

  @Benchmark @Group("no_contention") @GroupThreads(1)
  public void no_contention_drain(DrainCounters counters, ConsumerMarker cm) {
    q.drainTo(buffer -> {
      counters.drained++;
      return true;
    }, 1000);
  }

  @Benchmark @Group("mild_contention") @GroupThreads(2)
  public void mild_contention_offer(OfferCounters counters) {
    q.offer(message, counters.deferred);
  }

  @Benchmark @Group("mild_contention") @GroupThreads(1)
  public void mild_contention_drain(DrainCounters counters, ConsumerMarker cm) {
    q.drainTo(buffer -> {
      counters.drained++;
      return true;
    }, 1000);
  }

  @Benchmark @Group("high_contention") @GroupThreads(8)
  public void high_contention_offer(OfferCounters counters) {
    q.offer(message, counters.deferred);
  }

  @Benchmark @Group("high_contention") @GroupThreads(1)
  public void high_contention_drain(DrainCounters counters, ConsumerMarker cm) {
    q.drainTo(buffer -> {
      counters.drained++;
      return true;
    }, 1000);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + SizeBoundedQueueBenchmark.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
