package io.github.tramchamploo.bufferslayer;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Jdbc benchmark that simply doing inserts to a single table.
 */
public abstract class AbstractBatchJdbcTemplateBenchmark {

  private DriverManagerDataSource dataSource;
  private BatchJdbcTemplate batch;
  private JdbcTemplate unbatch;
  private Reporter<Sql, Integer> reporter;
  private static SenderProxy<Sql, Integer> proxy;
  private static AtomicLong counter = new AtomicLong();

  private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS test";
  private static final String CREATE_TABLE = "CREATE TABLE test.benchmark(id INT PRIMARY KEY AUTO_INCREMENT, data VARCHAR(32), time TIMESTAMP)";
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS test.benchmark";
  private static final String TRUNCATE_TABLE = "TRUNCATE TABLE test.benchmark";
  private static final String INSERTION = "INSERT INTO test.benchmark(data, time) VALUES(?, ?)";

  static String propertyOr(String key, String fallback) {
    return System.getProperty(key, fallback);
  }

  protected abstract Reporter<Sql, Integer> reporter(Sender<Sql, Integer> sender);

  @Setup
  public void setup() {
    dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName("com.mysql.jdbc.Driver");
    dataSource.setUrl(propertyOr("jdbcUrl", "jdbc:mysql://127.0.0.1:3306?useSSL=false"));
    dataSource.setUsername(propertyOr("username", "root"));
    dataSource.setPassword(propertyOr("password", "root"));

    JdbcTemplate delegate = new JdbcTemplate(dataSource);
    delegate.setDataSource(dataSource);

    proxy = new SenderProxy<>(new JdbcTemplateSender(delegate));
    proxy.onMessages(updated -> counter.addAndGet(updated.size()));

    reporter = reporter(proxy);
    batch = new BatchJdbcTemplate(delegate, reporter);
    batch.setDataSource(dataSource);

    unbatch = new JdbcTemplate(dataSource);
    unbatch.setDataSource(dataSource);
    unbatch.update(CREATE_DATABASE);
    unbatch.update(DROP_TABLE);
    unbatch.update(CREATE_TABLE);
  }

  @TearDown(Level.Iteration)
  public void dropTable() {
    unbatch.update(TRUNCATE_TABLE);
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
    batch.update(INSERTION, randomString(), new Date());
  }

  @Benchmark @Group("no_contention_unbatched") @GroupThreads(1)
  public void no_contention_unbatched_insert(Lagging l) {
    unbatch.update(INSERTION, randomString(), new Date());
  }

  @Benchmark @Group("mild_contention_batched") @GroupThreads(2)
  public void mild_contention_batched_insert(Lagging l, AtomicLongCounter counters) {
    batch.update(INSERTION, randomString(), new Date());
  }

  @Benchmark @Group("mild_contention_unbatched") @GroupThreads(2)
  public void mild_contention_unbatched_insert(Lagging l) {
    unbatch.update(INSERTION, randomString(), new Date());
  }

  @Benchmark @Group("high_contention_batched") @GroupThreads(8)
  public void high_contention_batched_insert(Lagging l, AtomicLongCounter counters) {
    batch.update(INSERTION, randomString(), new Date());
  }

  @Benchmark @Group("high_contention_unbatched") @GroupThreads(8)
  public void high_contention_unbatched_insert(Lagging l) {
    unbatch.update(INSERTION, randomString(), new Date());
  }
}
