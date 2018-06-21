package io.github.tramchamploo.bufferslayer;

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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Jedis benchmark that executing jedis commands.
 */
public abstract class AbstractBatchJedisBenchmark {

  private JedisPool unbatch;
  private BatchJedis batch;
  private Reporter<RedisCommand, Object> reporter;
  private static SenderProxy<RedisCommand, Object> proxy;
  private static AtomicLong counter = new AtomicLong();

  static String propertyOr(String key, String fallback) {
    return System.getProperty(key, fallback);
  }

  protected abstract Reporter<RedisCommand, Object> reporter(Sender<RedisCommand, Object> sender);

  @Setup
  public void setup() {
    unbatch = new JedisPool(propertyOr("redisHost", "127.0.0.1"),
        Integer.parseInt(propertyOr("redisPort", "6379")));

    proxy = new SenderProxy<>(new JedisSender(unbatch));
    proxy.onMessages(updated -> counter.addAndGet(updated.size()));

    reporter = reporter(proxy);
    batch = new BatchJedis(unbatch, reporter);
  }

  @TearDown(Level.Iteration)
  public void flushDB() {
    try (Jedis jedis = unbatch.getResource()) {
      jedis.flushDB();
    }
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
      TimeUnit.SECONDS.sleep(1);
    }
  }

  static String randomString() {
    return String.valueOf(ThreadLocalRandom.current().nextLong());
  }

  @Benchmark @Group("no_contention_batched") @GroupThreads(1)
  public void no_contention_batched_set(Lagging l, AtomicLongCounter counters) {
    batch.set(randomString(), randomString());
  }

  @Benchmark @Group("no_contention_unbatched") @GroupThreads(1)
  public void no_contention_unbatched_set(Lagging l) {
    try (Jedis jedis = unbatch.getResource()) {
      jedis.set(randomString(), randomString());
    }
  }

  @Benchmark @Group("mild_contention_batched") @GroupThreads(2)
  public void mild_contention_batched_set(Lagging l, AtomicLongCounter counters) {
    batch.set(randomString(), randomString());
  }

  @Benchmark @Group("mild_contention_unbatched") @GroupThreads(2)
  public void mild_contention_unbatched_set(Lagging l) {
    try (Jedis jedis = unbatch.getResource()) {
      jedis.set(randomString(), randomString());
    }
  }

  @Benchmark @Group("high_contention_batched") @GroupThreads(8)
  public void high_contention_batched_set(Lagging l, AtomicLongCounter counters) {
    batch.set(randomString(), randomString());
  }

  @Benchmark @Group("high_contention_unbatched") @GroupThreads(8)
  public void high_contention_unbatched_set(Lagging l) {
    try (Jedis jedis = unbatch.getResource()) {
      jedis.set(randomString(), randomString());
    }
  }
}
