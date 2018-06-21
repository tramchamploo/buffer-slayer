package io.github.tramchamploo.bufferslayer;

import java.util.List;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

/**
 * Send buffered commands in jedis's pipeline
 */
final class JedisSender implements Sender<RedisCommand, Object> {

  private final JedisPool jedisPool;

  JedisSender(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  @Override
  public CheckResult check() {
    Jedis jedis = null;
    try {
      jedis = jedisPool.getResource();

      String ping = jedis.ping();
      if ("PONG".equalsIgnoreCase(ping)) {
        return CheckResult.OK;
      }

      return CheckResult.failed(new RuntimeException("PING doesn't get PONG."));
    } catch (Exception e) {
      return CheckResult.failed(e);
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }

  @Override
  public void close() {
    jedisPool.close();
  }

  @Override
  public List<Object> send(List<RedisCommand> messages) {
    Jedis jedis = null;
    try {
      jedis = jedisPool.getResource();

      Pipeline pipeline = jedis.pipelined();
      for (RedisCommand command : messages) {
        command.apply(pipeline);
      }

      return pipeline.syncAndReturnAll();
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }
}
