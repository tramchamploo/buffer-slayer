package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.MessageFuture;
import java.util.concurrent.TimeUnit;
import junit.framework.AssertionFailedError;
import org.junit.Before;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public abstract class BatchJedisTestBase {

  protected final byte[] bfoo = {0x01, 0x02, 0x03, 0x04};
  protected final byte[] bbar = {0x05, 0x06, 0x07, 0x08};
  protected final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };

  protected Jedis jedis;
  protected BatchJedis batchJedis;
  protected AsyncReporter reporter;
  protected JedisPool jedisPool;

  protected static HostAndPort hnp = new HostAndPort("localhost", 6379);

  @Before
  public void setup() {
    jedis = new Jedis("localhost", 6379);
    jedis.flushAll();

    jedisPool = new JedisPool();
    reporter = AsyncReporter.builder(new JedisSender(jedisPool))
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();
    batchJedis = new BatchJedis(jedisPool, reporter);
  }

  protected Jedis createJedis() {
    Jedis j = new Jedis(hnp.getHost(), hnp.getPort());
    j.connect();
    j.flushAll();
    return j;
  }

  @SuppressWarnings("unchecked")
  <T> T blocking(MessageFuture<T> future) {
    reporter.flush();

    try {
      return future.get();
    } catch (Exception e) {
      throw new AssertionFailedError(e.getMessage());
    }
  }
}
