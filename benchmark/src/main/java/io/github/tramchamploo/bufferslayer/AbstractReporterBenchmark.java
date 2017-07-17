package io.github.tramchamploo.bufferslayer;

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
import org.openjdk.jmh.annotations.Warmup;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Group)
@SuppressWarnings("unchecked")
public abstract class AbstractReporterBenchmark {

  static Sender sender = new NoopSender();
  static InMemoryReporterMetrics metrics =
      InMemoryReporterMetrics.instance(ReporterMetricsExporter.NOOP_EXPORTER);

  Reporter reporter;

  static Message message() {
    return TestMessage.newMessage(0);
  }

  protected abstract Reporter getReporter();

  @Setup
  public void setup() {
    reporter = getReporter();
  }

  @AuxCounters
  @State(Scope.Thread)
  public static class InMemoryReporterMetricsAsCounters {

    public long messages() {
      return metrics.messages();
    }

    public long messagesDropped() {
      return metrics.messagesDropped();
    }

    public long queuedMessages() {
      return metrics.queuedMessages();
    }

    @Setup(Level.Iteration)
    public void clean() {
      metrics.clear();
    }
  }

  @Benchmark @Group("no_contention") @GroupThreads(1)
  public void no_contention_report(InMemoryReporterMetricsAsCounters counters) {
    reporter.report(message());
  }

  @Benchmark @Group("mild_contention") @GroupThreads(2)
  public void mild_contention_report(InMemoryReporterMetricsAsCounters counters) {
    reporter.report(message());
  }

  @Benchmark @Group("high_contention") @GroupThreads(8)
  public void high_contention_report(InMemoryReporterMetricsAsCounters counters) {
    reporter.report(message());
  }
}
