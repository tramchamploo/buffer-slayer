package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.internal.MessageFuture;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

public abstract class AbstractSizeBoundedQueueBenchmark {

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

  private AbstractSizeBoundedQueue q;

  @Setup
  public void setup() {
    q = newQueue(10000, OverflowStrategy.fail, Message.SINGLE_KEY);
    q._setBenchmarkMode(true);
  }

  protected abstract AbstractSizeBoundedQueue newQueue(
      int maxSize, OverflowStrategy.Strategy strategy, MessageKey key);

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
}
