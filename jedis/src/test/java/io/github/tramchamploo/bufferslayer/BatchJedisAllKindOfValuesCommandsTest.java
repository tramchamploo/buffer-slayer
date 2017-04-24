package io.github.tramchamploo.bufferslayer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BatchJedisAllKindOfValuesCommandsTest extends BatchJedisTestBase {

  @Test
  public void move() {
    long status = blocking(batchJedis.move("foo", 1));
    assertEquals(0, status);

    jedis.set("foo", "bar");
    status = blocking(batchJedis.move("foo", 1));
    assertEquals(1, status);
    assertNull(jedis.get("foo"));

    jedis.select(1);
    assertEquals("bar", jedis.get("foo"));

    // Binary
    jedis.select(0);
    long bstatus = blocking(batchJedis.move(bfoo, 1));
    assertEquals(0, bstatus);

    jedis.set(bfoo, bbar);
    bstatus = blocking(batchJedis.move(bfoo, 1));
    assertEquals(1, bstatus);
    assertNull(jedis.get(bfoo));

    jedis.select(1);
    assertArrayEquals(bbar, jedis.get(bfoo));
  }

  @Test
  public void persist() {
    jedis.setex("foo", 60 * 60, "bar");
    assertTrue(jedis.ttl("foo") > 0);
    long status = blocking(batchJedis.persist("foo"));
    assertEquals(1, status);
    assertEquals(-1, jedis.ttl("foo").intValue());

    // Binary
    jedis.setex(bfoo, 60 * 60, bbar);
    assertTrue(jedis.ttl(bfoo) > 0);
    long bstatus = blocking(batchJedis.persist(bfoo));
    assertEquals(1, bstatus);
    assertEquals(-1, jedis.ttl(bfoo).intValue());
  }
}
