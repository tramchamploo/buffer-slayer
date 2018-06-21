package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.AssertUtil.arrayContains;
import static io.github.tramchamploo.bufferslayer.AssertUtil.assertByteArrayListEquals;
import static io.github.tramchamploo.bufferslayer.AssertUtil.assertByteArraySetEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.tramchamploo.bufferslayer.internal.MessageFuture;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.SafeEncoder;

public class BatchJedisTest extends BatchJedisTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void pipeline() {
    JedisPool jedisPool = mock(JedisPool.class);
    reporter = AsyncReporter.builder(new JedisSender(jedisPool))
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build();

    batchJedis = new BatchJedis(jedisPool, reporter);

    Jedis jedis = mock(Jedis.class);
    when(jedisPool.getResource()).thenReturn(jedis);

    Pipeline pipeline = mock(Pipeline.class);
    when(pipeline.syncAndReturnAll()).thenReturn(Arrays.asList(1L, 1L));
    when(jedis.pipelined()).thenReturn(pipeline);

    batchJedis.append("foo", "bar");
    batchJedis.append("foo", "bar");
    reporter.flush();

    verify(jedis).pipelined();
    verify(pipeline, times(2))
        .append(SafeEncoder.encode("foo"), SafeEncoder.encode("bar"));
    verify(pipeline).syncAndReturnAll();
  }

  @Test
  public void pipelineWithError() throws ExecutionException, InterruptedException {
    jedis.set("foo", "bar");
    MessageFuture<Long> barFuture = batchJedis.incr("bar");
    MessageFuture<Long> fooFuture = batchJedis.incr("foo");
    reporter.flush();
    assertEquals(1, barFuture.get().intValue());
    try {
      fooFuture.get();
    } catch (Throwable t) {
      assertEquals(JedisDataException.class, t.getCause().getCause().getClass());
    }
  }

  @Test
  public void append() {
    long value = blocking(batchJedis.append("foo", "bar"));
    assertEquals(3L, value);
    assertEquals("bar", jedis.get("foo"));

    value = blocking(batchJedis.append("foo", "bar"));
    assertEquals(6L, value);
    assertEquals("barbar", jedis.get("foo"));
  }

  @Test
  public void blpop() {
    List<String> value = blocking(batchJedis.blpop(1, "foo"));
    assertNull(value);

    jedis.lpush("foo", "bar");
    value = blocking(batchJedis.blpop(1, "foo"));
    assertEquals(2, value.size());
    assertEquals("foo", value.get(0));
    assertEquals("bar", value.get(1));

    // Binary
    jedis.lpush(bfoo, bbar);
    List<byte[]> value2 = blocking(batchJedis.blpop(1, bfoo));
    assertEquals(2, value2.size());
    assertArrayEquals(bfoo, value2.get(0));
    assertArrayEquals(bbar, value2.get(1));
  }

  @Test
  public void brpop() {
    List<String> value = blocking(batchJedis.brpop(1, "foo"));
    assertNull(value);

    jedis.lpush("foo", "bar");
    value = blocking(batchJedis.brpop(1, "foo"));
    assertEquals(2, value.size());
    assertEquals("foo", value.get(0));
    assertEquals("bar", value.get(1));

    // Binary
    jedis.lpush(bfoo, bbar);
    List<byte[]> value2 = blocking(batchJedis.brpop(1, bfoo));
    assertEquals(2, value2.size());
    assertArrayEquals(bfoo, value2.get(0));
    assertArrayEquals(bbar, value2.get(1));
  }

  @Test
  public void decr() {
    assertEquals(new Long(1L), jedis.incr("foo"));
    assertEquals(new Long(0L), blocking(batchJedis.decr("foo")));

    // Binary
    assertEquals(new Long(1L), jedis.incr(bfoo));
    assertEquals(new Long(0L), blocking(batchJedis.decr(bfoo)));
  }

  @Test
  public void decrBy() {
    assertEquals(new Long(2L), jedis.incrBy("foo", 2));
    assertEquals(new Long(0L), blocking(batchJedis.decrBy("foo", 2)));

    // Binary
    assertEquals(new Long(2L), jedis.incrBy(bfoo, 2));
    assertEquals(new Long(0L), blocking(batchJedis.decrBy(bfoo, 2)));
  }

  @Test
  public void del() {
    assertEquals("OK", jedis.set("foo", "bar"));
    assertEquals(new Long(1L), blocking(batchJedis.del("foo")));

    // Binary
    assertEquals("OK", jedis.set(bfoo, bbar));
    assertEquals(new Long(1L), blocking(batchJedis.del(bfoo)));
  }

  @Test
  public void echo() {
    assertEquals("foo", blocking(batchJedis.echo("foo")));

    // Binary
    assertArrayEquals(bfoo, blocking(batchJedis.echo(bfoo)));
  }

  @Test
  public void exists() {
    assertEquals("OK", jedis.set("foo", "bar"));
    assertEquals(true, blocking(batchJedis.exists("foo")));

    assertEquals("OK", jedis.set("bar", "bar"));
    assertEquals(new Long(2L), blocking(batchJedis.exists("foo", "bar")));

    // Binary
    assertEquals("OK", jedis.set(bfoo, bbar));
    assertEquals(true, blocking(batchJedis.exists(bfoo)));

    assertEquals("OK", jedis.set(bbar, bbar));
    assertEquals(new Long(2L), blocking(batchJedis.exists(bfoo, bbar)));
  }

  @Test
  public void expire() throws InterruptedException {
    assertEquals("OK", jedis.set("foo", "bar"));
    assertEquals(new Long(1L), blocking(batchJedis.expire("foo", 1)));
    Thread.sleep(1200);
    assertFalse(jedis.exists("foo"));

    // Binary
    assertEquals("OK", jedis.set(bfoo, bbar));
    assertEquals(new Long(1L), blocking(batchJedis.expire(bfoo, 1)));
    Thread.sleep(1200);
    assertFalse(jedis.exists(bfoo));
  }

  @Test
  public void expireAt() throws InterruptedException {
    assertEquals("OK", jedis.set("foo", "bar"));
    assertEquals(new Long(1L), blocking(
        batchJedis.expireAt("foo", System.currentTimeMillis() / 1000 + 1)));
    Thread.sleep(1000);
    assertFalse(jedis.exists("foo"));

    // Binary
    assertEquals("OK", jedis.set(bfoo, bbar));
    assertEquals(new Long(1L), blocking(
        batchJedis.expireAt(bfoo, System.currentTimeMillis() / 1000 + 1)));
    Thread.sleep(1000);
    assertFalse(jedis.exists(bfoo));
  }

  @Test
  public void get() {
    assertEquals("OK", jedis.set("foo", "bar"));
    assertEquals("bar", blocking(batchJedis.get("foo")));

    // Binary
    assertEquals("OK", jedis.set(bfoo, bbar));
    assertArrayEquals(bbar, blocking(batchJedis.get(bfoo)));
  }

  @Test
  public void getbit() {
    boolean bit = jedis.setbit("foo", 0, true);
    assertEquals(false, bit);

    bit = blocking(batchJedis.getbit("foo", 0));
    assertEquals(true, bit);

    boolean bbit = jedis.setbit("bfoo".getBytes(), 0, "1".getBytes());
    assertFalse(bbit);

    bbit = blocking(batchJedis.getbit("bfoo".getBytes(), 0));
    assertTrue(bbit);
  }

  @Test
  public void bitpos() {
    String foo = "foo";

    jedis.set(foo, String.valueOf(0));

    jedis.setbit(foo, 3, true);
    jedis.setbit(foo, 7, true);
    jedis.setbit(foo, 13, true);
    jedis.setbit(foo, 39, true);

    /*
     * byte: 0 1 2 3 4 bit: 00010001 / 00000100 / 00000000 / 00000000 / 00000001
     */
    long offset = blocking(batchJedis.bitpos(foo, true));
    assertEquals(2, offset);
    offset = blocking(batchJedis.bitpos(foo, false));
    assertEquals(0, offset);

    offset = blocking(batchJedis.bitpos(foo, true, new redis.clients.jedis.BitPosParams(1)));
    assertEquals(13, offset);
    offset = blocking(batchJedis.bitpos(foo, false, new redis.clients.jedis.BitPosParams(1)));
    assertEquals(8, offset);

    offset = blocking(batchJedis.bitpos(foo, true, new redis.clients.jedis.BitPosParams(2, 3)));
    assertEquals(-1, offset);
    offset = blocking(batchJedis.bitpos(foo, false, new redis.clients.jedis.BitPosParams(2, 3)));
    assertEquals(16, offset);

    offset = blocking(batchJedis.bitpos(foo, true, new redis.clients.jedis.BitPosParams(3, 4)));
    assertEquals(39, offset);
  }

  @Test
  public void bitposBinary() {
    // binary
    byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };

    jedis.set(bfoo, Protocol.toByteArray(0));

    jedis.setbit(bfoo, 3, true);
    jedis.setbit(bfoo, 7, true);
    jedis.setbit(bfoo, 13, true);
    jedis.setbit(bfoo, 39, true);

    /*
     * byte: 0 1 2 3 4 bit: 00010001 / 00000100 / 00000000 / 00000000 / 00000001
     */
    long offset = blocking(batchJedis.bitpos(bfoo, true));
    assertEquals(2, offset);
    offset = blocking(batchJedis.bitpos(bfoo, false));
    assertEquals(0, offset);

    offset = blocking(batchJedis.bitpos(bfoo, true, new redis.clients.jedis.BitPosParams(1)));
    assertEquals(13, offset);
    offset = blocking(batchJedis.bitpos(bfoo, false, new redis.clients.jedis.BitPosParams(1)));
    assertEquals(8, offset);

    offset = blocking(batchJedis.bitpos(bfoo, true, new redis.clients.jedis.BitPosParams(2, 3)));
    assertEquals(-1, offset);
    offset = blocking(batchJedis.bitpos(bfoo, false, new redis.clients.jedis.BitPosParams(2, 3)));
    assertEquals(16, offset);

    offset = blocking(batchJedis.bitpos(bfoo, true, new redis.clients.jedis.BitPosParams(3, 4)));
    assertEquals(39, offset);
  }

  @Test
  public void bitposWithNoMatchingBitExist() {
    String foo = "foo";

    jedis.set(foo, String.valueOf(0));
    for (int idx = 0; idx < 8; idx++) {
      jedis.setbit(foo, idx, true);
    }

    /*
     * byte: 0 bit: 11111111
     */
    long offset = blocking(batchJedis.bitpos(foo, false));
    // offset should be last index + 1
    assertEquals(8, offset);
  }

  @Test
  public void bitposWithNoMatchingBitExistWithinRange() {
    String foo = "foo";

    jedis.set(foo, String.valueOf(0));
    for (int idx = 0; idx < 8 * 5; idx++) {
      jedis.setbit(foo, idx, true);
    }

    /*
     * byte: 0 1 2 3 4 bit: 11111111 / 11111111 / 11111111 / 11111111 / 11111111
     */
    long offset = blocking(batchJedis.bitpos(foo, false, new BitPosParams(2, 3)));
    // offset should be -1
    assertEquals(-1, offset);
  }

  @Test
  public void getRange() {
    assertEquals("OK", jedis.set("foo", "bar"));
    assertEquals("a", blocking(batchJedis.getrange("foo", 1, 1)));

    // Binary
    assertEquals("OK", jedis.set(bfoo, bbar));
    assertArrayEquals(new byte[]{0x06}, blocking(batchJedis.getrange(bfoo, 1, 1)));
  }

  @Test
  public void setAndGet() {
    String status = blocking(batchJedis.set("foo", "bar"));
    assertEquals("OK", status);

    String value = jedis.get("foo");
    assertEquals("bar", value);

    // Binary
    status = blocking(batchJedis.set(bfoo, bbar));
    assertEquals("OK", status);
    byte[] bvalue = jedis.get(bfoo);
    assertArrayEquals(bbar, bvalue);
  }

  @Test
  public void getSet() {
    String status = jedis.set("foo", "bar");
    assertEquals("OK", status);

    String value = blocking(batchJedis.getSet("foo", "foo"));
    assertEquals("bar", value);
    assertEquals("foo", jedis.get("foo"));

    // Binary
    status = jedis.set(bfoo, bbar);
    assertEquals("OK", status);
    byte[] bvalue = blocking(batchJedis.getSet(bfoo, bfoo));
    assertArrayEquals(bbar, bvalue);
    assertArrayEquals(bfoo, jedis.get(bfoo));
  }

  @Test
  public void hdel() {
    Map<String, String> hash = new HashMap<>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedis.hmset("foo", hash);

    assertEquals(0, blocking(batchJedis.hdel("bar", "foo")).intValue());
    assertEquals(0, blocking(batchJedis.hdel("foo", "foo")).intValue());
    assertEquals(1, blocking(batchJedis.hdel("foo", "bar")).intValue());
    assertEquals(null, jedis.hget("foo", "bar"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedis.hmset(bfoo, bhash);

    assertEquals(0, blocking(batchJedis.hdel(bbar, bfoo)).intValue());
    assertEquals(0, blocking(batchJedis.hdel(bfoo, bfoo)).intValue());
    assertEquals(1, blocking(batchJedis.hdel(bfoo, bbar)).intValue());
    assertEquals(null, jedis.hget(bfoo, bbar));
  }

  @Test
  public void hexists() {
    Map<String, String> hash = new HashMap<>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedis.hmset("foo", hash);

    assertFalse(blocking(batchJedis.hexists("bar", "foo")));
    assertFalse(blocking(batchJedis.hexists("foo", "foo")));
    assertTrue(blocking(batchJedis.hexists("foo", "bar")));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedis.hmset(bfoo, bhash);

    assertFalse(blocking(batchJedis.hexists(bbar, bfoo)));
    assertFalse(blocking(batchJedis.hexists(bfoo, bfoo)));
    assertTrue(blocking(batchJedis.hexists(bfoo, bbar)));
  }

  @Test
  public void hget() {
    jedis.hset("foo", "bar", "car");
    assertEquals(null, blocking(batchJedis.hget("bar", "foo")));
    assertEquals(null, blocking(batchJedis.hget("foo", "car")));
    assertEquals("car", blocking(batchJedis.hget("foo", "bar")));

    // Binary
    jedis.hset(bfoo, bbar, bcar);
    assertEquals(null, blocking(batchJedis.hget(bbar, bfoo)));
    assertEquals(null, blocking(batchJedis.hget(bfoo, bcar)));
    assertArrayEquals(bcar, blocking(batchJedis.hget(bfoo, bbar)));
  }

  @Test
  public void hgetAll() {
    Map<String, String> h = new HashMap<>();
    h.put("bar", "car");
    h.put("car", "bar");
    jedis.hmset("foo", h);

    Map<String, String> hash = blocking(batchJedis.hgetAll("foo"));
    assertEquals(2, hash.size());
    assertEquals("car", hash.get("bar"));
    assertEquals("bar", hash.get("car"));

    // Binary
    Map<byte[], byte[]> bh = new HashMap<>();
    bh.put(bbar, bcar);
    bh.put(bcar, bbar);
    jedis.hmset(bfoo, bh);
    Map<byte[], byte[]> bhash = blocking(batchJedis.hgetAll(bfoo));

    assertEquals(2, bhash.size());
    assertArrayEquals(bcar, bhash.get(bbar));
    assertArrayEquals(bbar, bhash.get(bcar));
  }

  @Test
  public void hincrBy() {
    long value = blocking(batchJedis.hincrBy("foo", "bar", 1));
    assertEquals(1, value);
    value = blocking(batchJedis.hincrBy("foo", "bar", -1));
    assertEquals(0, value);
    value = blocking(batchJedis.hincrBy("foo", "bar", -10));
    assertEquals(-10, value);

    // Binary
    long bvalue = blocking(batchJedis.hincrBy(bfoo, bbar, 1));
    assertEquals(1, bvalue);
    bvalue = blocking(batchJedis.hincrBy(bfoo, bbar, -1));
    assertEquals(0, bvalue);
    bvalue = blocking(batchJedis.hincrBy(bfoo, bbar, -10));
    assertEquals(-10, bvalue);
  }

  @Test
  public void hkeys() {
    Map<String, String> hash = new LinkedHashMap<>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedis.hmset("foo", hash);

    Set<String> keys = blocking(batchJedis.hkeys("foo"));
    Set<String> expected = new LinkedHashSet<>();
    expected.add("bar");
    expected.add("car");
    assertEquals(expected, keys);

    // Binary
    Map<byte[], byte[]> bhash = new LinkedHashMap<>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedis.hmset(bfoo, bhash);

    Set<byte[]> bkeys = blocking(batchJedis.hkeys(bfoo));
    Set<byte[]> bexpected = new LinkedHashSet<>();
    bexpected.add(bbar);
    bexpected.add(bcar);
    assertByteArraySetEquals(bexpected, bkeys);
  }

  @Test
  public void hlen() {
    Map<String, String> hash = new HashMap<>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedis.hmset("foo", hash);

    assertEquals(0, blocking(batchJedis.hlen("bar")).intValue());
    assertEquals(2, blocking(batchJedis.hlen("foo")).intValue());

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedis.hmset(bfoo, bhash);

    assertEquals(0, blocking(batchJedis.hlen(bbar)).intValue());
    assertEquals(2, blocking(batchJedis.hlen(bfoo)).intValue());
  }

  @Test
  public void hmget() {
    Map<String, String> hash = new HashMap<>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedis.hmset("foo", hash);

    List<String> values = blocking(batchJedis.hmget("foo", "bar", "car", "foo"));
    List<String> expected = new ArrayList<>();
    expected.add("car");
    expected.add("bar");
    expected.add(null);

    assertEquals(expected, values);

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedis.hmset(bfoo, bhash);

    List<byte[]> bvalues = blocking(batchJedis.hmget(bfoo, bbar, bcar, bfoo));
    List<byte[]> bexpected = new ArrayList<>();
    bexpected.add(bcar);
    bexpected.add(bbar);
    bexpected.add(null);

    assertByteArrayListEquals(bexpected, bvalues);
  }

  @Test
  public void hmset() {
    Map<String, String> hash = new HashMap<>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    String status = blocking(batchJedis.hmset("foo", hash));
    assertEquals("OK", status);
    assertEquals("car", jedis.hget("foo", "bar"));
    assertEquals("bar", jedis.hget("foo", "car"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    String bstatus = blocking(batchJedis.hmset(bfoo, bhash));
    assertEquals("OK", bstatus);
    assertArrayEquals(bcar, jedis.hget(bfoo, bbar));
    assertArrayEquals(bbar, jedis.hget(bfoo, bcar));
  }

  @Test
  public void hset() {
    long status = blocking(batchJedis.hset("foo", "bar", "car"));
    assertEquals(1, status);
    status = blocking(batchJedis.hset("foo", "bar", "foo"));
    assertEquals(0, status);

    // Binary
    long bstatus = blocking(batchJedis.hset(bfoo, bbar, bcar));
    assertEquals(1, bstatus);
    bstatus = blocking(batchJedis.hset(bfoo, bbar, bfoo));
    assertEquals(0, bstatus);
  }

  @Test
  public void hsetnx() {
    long status = blocking(batchJedis.hsetnx("foo", "bar", "car"));
    assertEquals(1, status);
    assertEquals("car", jedis.hget("foo", "bar"));

    status = blocking(batchJedis.hsetnx("foo", "bar", "foo"));
    assertEquals(0, status);
    assertEquals("car", jedis.hget("foo", "bar"));

    status = blocking(batchJedis.hsetnx("foo", "car", "bar"));
    assertEquals(1, status);
    assertEquals("bar", jedis.hget("foo", "car"));

    // Binary
    long bstatus = blocking(batchJedis.hsetnx(bfoo, bbar, bcar));
    assertEquals(1, bstatus);
    assertArrayEquals(bcar, jedis.hget(bfoo, bbar));

    bstatus = blocking(batchJedis.hsetnx(bfoo, bbar, bfoo));
    assertEquals(0, bstatus);
    assertArrayEquals(bcar, jedis.hget(bfoo, bbar));

    bstatus = blocking(batchJedis.hsetnx(bfoo, bcar, bbar));
    assertEquals(1, bstatus);
    assertArrayEquals(bbar, jedis.hget(bfoo, bcar));
  }

  @Test
  public void hvals() {
    Map<String, String> hash = new LinkedHashMap<>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    jedis.hmset("foo", hash);

    List<String> vals = blocking(batchJedis.hvals("foo"));
    assertEquals(2, vals.size());
    assertTrue(vals.contains("bar"));
    assertTrue(vals.contains("car"));

    // Binary
    Map<byte[], byte[]> bhash = new LinkedHashMap<>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    jedis.hmset(bfoo, bhash);

    List<byte[]> bvals = blocking(batchJedis.hvals(bfoo));

    assertEquals(2, bvals.size());
    assertTrue(arrayContains(bvals, bbar));
    assertTrue(arrayContains(bvals, bcar));
  }

  @Test
  public void incrWrongValue() {
    jedis.set("foo", "bar");
    try {
      MessageFuture<Long> future = batchJedis.incr("foo");
      reporter.flush();
      future.get();
    } catch (Throwable t) {
      assertEquals(JedisDataException.class, t.getCause().getCause().getClass());
    }
  }

  @Test
  public void incr() {
    long value = blocking(batchJedis.incr("foo"));
    assertEquals(1, value);
    value = blocking(batchJedis.incr("foo"));
    assertEquals(2, value);
  }

  @Test
  public void incrByWrongValue() {
    jedis.set("foo", "bar");
    try {
      MessageFuture<Long> future = batchJedis.incrBy("foo", 2);
      reporter.flush();
      future.get();
    } catch (Throwable t) {
      assertEquals(JedisDataException.class, t.getCause().getCause().getClass());
    }
  }

  @Test
  public void incrBy() {
    long value = blocking(batchJedis.incrBy("foo", 2));
    assertEquals(2, value);
    value = blocking(batchJedis.incrBy("foo", 2));
    assertEquals(4, value);
  }

  @Test
  public void mget() {
    List<String> values = blocking(batchJedis.mget("foo", "bar"));
    List<String> expected = new ArrayList<>();
    expected.add(null);
    expected.add(null);

    assertEquals(expected, values);

    jedis.set("foo", "bar");

    expected = new ArrayList<>();
    expected.add("bar");
    expected.add(null);
    values = blocking(batchJedis.mget("foo", "bar"));

    assertEquals(expected, values);

    jedis.set("bar", "foo");

    expected = new ArrayList<>();
    expected.add("bar");
    expected.add("foo");
    values = blocking(batchJedis.mget("foo", "bar"));

    assertEquals(expected, values);
  }

  @Test
  public void mset() {
    String status = blocking(batchJedis.mset("foo", "bar", "bar", "foo"));
    assertEquals("OK", status);
    assertEquals("bar", jedis.get("foo"));
    assertEquals("foo", jedis.get("bar"));
  }

  @Test
  public void msetnx() {
    long status = blocking(batchJedis.msetnx("foo", "bar", "bar", "foo"));
    assertEquals(1, status);
    assertEquals("bar", jedis.get("foo"));
    assertEquals("foo", jedis.get("bar"));

    status = blocking(batchJedis.msetnx("foo", "bar1", "bar2", "foo2"));
    assertEquals(0, status);
    assertEquals("bar", jedis.get("foo"));
    assertEquals("foo", jedis.get("bar"));
  }

  @Test
  public void rename() {
    jedis.set("foo", "bar");
    String status = blocking(batchJedis.rename("foo", "bar"));
    assertEquals("OK", status);

    String value = jedis.get("foo");
    assertEquals(null, value);

    value = jedis.get("bar");
    assertEquals("bar", value);

    // Binary
    jedis.set(bfoo, bbar);
    String bstatus = blocking(batchJedis.rename(bfoo, bbar));
    assertEquals("OK", bstatus);

    byte[] bvalue = jedis.get(bfoo);
    assertEquals(null, bvalue);

    bvalue = jedis.get(bbar);
    assertArrayEquals(bbar, bvalue);
  }

  @Test
  public void renameOldAndNewAreTheSame() {
    jedis.set("foo", "bar");
    blocking(batchJedis.rename("foo", "foo"));

    // Binary
    jedis.set(bfoo, bbar);
    blocking(batchJedis.rename(bfoo, bfoo));
  }

  @Test
  public void renamenx() {
    jedis.set("foo", "bar");
    long status = blocking(batchJedis.renamenx("foo", "bar"));
    assertEquals(1, status);

    jedis.set("foo", "bar");
    status = blocking(batchJedis.renamenx("foo", "bar"));
    assertEquals(0, status);

    // Binary
    jedis.set(bfoo, bbar);
    long bstatus = blocking(batchJedis.renamenx(bfoo, bbar));
    assertEquals(1, bstatus);

    jedis.set(bfoo, bbar);
    bstatus = blocking(batchJedis.renamenx(bfoo, bbar));
    assertEquals(0, bstatus);
  }
}
