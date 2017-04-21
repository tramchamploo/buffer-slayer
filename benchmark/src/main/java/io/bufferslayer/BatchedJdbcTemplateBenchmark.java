package io.bufferslayer;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Created by guohang.bao on 2017/3/16.
 */
@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 3, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Group)
public class BatchedJdbcTemplateBenchmark {

  private ComboPooledDataSource dataSource;
  private BatchedJdbcTemplate batched;
  private JdbcTemplate unbatched;
  private AsyncReporter reporter;
  private static SenderProxy proxy;
  private static AtomicLong counter = new AtomicLong();

  private static final String CREATE_TABLE = "CREATE TABLE benchmark(id INT PRIMARY KEY AUTO_INCREMENT, data VARCHAR(32), time TIMESTAMP);";
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS benchmark;";
  private static final String TRUNCATE_TABLE = "TRUNCATE TABLE benchmark;";
  private static final String INSERTION = "INSERT INTO benchmark(data, time) VALUES(?, ?);";

  @Setup
  public void setup() throws PropertyVetoException {
    dataSource = new ComboPooledDataSource();
    dataSource.setDriverClass("org.h2.Driver");
    dataSource.setMinPoolSize(10);
    dataSource.setMaxPoolSize(50);
    dataSource.setJdbcUrl("jdbc:h2:~/benchmark");

    JdbcTemplate delegate = new JdbcTemplate(dataSource);
    delegate.setDataSource(dataSource);

    proxy = new SenderProxy(new JdbcTemplateSender(delegate));
    proxy.onMessages(updated -> counter.addAndGet(updated.size()));

    reporter = AsyncReporter.builder(proxy)
        .flushThreadKeepalive(1, TimeUnit.SECONDS)
        .strictOrder(true)
        .build();
    batched = new BatchedJdbcTemplate(delegate, reporter);
    batched.setDataSource(dataSource);

    unbatched = new JdbcTemplate(dataSource);
    unbatched.setDataSource(dataSource);
    unbatched.update(DROP_TABLE);
    unbatched.update(CREATE_TABLE);
  }

  @TearDown(Level.Iteration)
  public void dropTable() {
    reporter.flush();
    unbatched.update(TRUNCATE_TABLE);
  }

  @AuxCounters
  @State(Scope.Thread)
  public static class AtomicLongCounter {

    public long updated() {
      return counter.get();
    }

    @Setup(Level.Iteration)
    public void clean() {
      counter.set(0);
    }
  }

  @State(Scope.Benchmark)
  public static class Lagging {

    @Setup(Level.Iteration)
    public void lag() throws InterruptedException {
      TimeUnit.SECONDS.sleep(3);
    }
  }

  static String randomString() {
    return String.valueOf(ThreadLocalRandom.current().nextLong());
  }

  @Benchmark @Group("no_contention_batched") @GroupThreads(1)
  public void no_contention_batched_insert(Lagging l, AtomicLongCounter counters) {
    batched.update(INSERTION, new Object[]{randomString(), new Date()});
  }

  @Benchmark @Group("no_contention_unbatched") @GroupThreads(1)
  public void no_contention_unbatched_insert(Lagging l) {
    unbatched.update(INSERTION, new Object[]{randomString(), new Date()});
  }

  @Benchmark @Group("mild_contention_batched") @GroupThreads(2)
  public void mild_contention_batched_insert(Lagging l, AtomicLongCounter counters) {
    batched.update(INSERTION, new Object[]{randomString(), new Date()});
  }

  @Benchmark @Group("mild_contention_unbatched") @GroupThreads(2)
  public void mild_contention_unbatched_insert(Lagging l) {
    unbatched.update(INSERTION, new Object[]{randomString(), new Date()});
  }

  @Benchmark @Group("high_contention_batched") @GroupThreads(8)
  public void high_contention_batched_insert(Lagging l, AtomicLongCounter counters) {
    batched.update(INSERTION, new Object[]{randomString(), new Date()});
  }

  @Benchmark @Group("high_contention_unbatched") @GroupThreads(8)
  public void high_contention_unbatched_insert(Lagging l) {
    unbatched.update(INSERTION, new Object[]{randomString(), new Date()});
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + BatchedJdbcTemplateBenchmark.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
