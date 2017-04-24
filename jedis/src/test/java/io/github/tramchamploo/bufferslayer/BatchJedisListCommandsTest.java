package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.AssertUtil.assertByteArrayListEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.github.tramchamploo.bufferslayer.internal.MessageFuture;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;

public class BatchJedisListCommandsTest extends BatchJedisTestBase {

  final byte[] bA = { 0x0A };
  final byte[] bB = { 0x0B };
  final byte[] bC = { 0x0C };
  final byte[] bdst = { 0x11, 0x12, 0x13, 0x14 };
  final byte[] b1 = { 0x01 };
  final byte[] b2 = { 0x02 };
  final byte[] b3 = { 0x03 };
  final byte[] bhello = { 0x04, 0x02 };
  final byte[] bx = { 0x02, 0x04 };

  @Test
  public void rpoplpush() {
    jedis.rpush("foo", "a");
    jedis.rpush("foo", "b");
    jedis.rpush("foo", "c");

    jedis.rpush("dst", "foo");
    jedis.rpush("dst", "bar");

    String element = blocking(batchJedis.rpoplpush("foo", "dst"));

    assertEquals("c", element);

    List<String> srcExpected = new ArrayList<String>();
    srcExpected.add("a");
    srcExpected.add("b");

    List<String> dstExpected = new ArrayList<String>();
    dstExpected.add("c");
    dstExpected.add("foo");
    dstExpected.add("bar");

    assertEquals(srcExpected, jedis.lrange("foo", 0, 1000));
    assertEquals(dstExpected, jedis.lrange("dst", 0, 1000));

    // Binary
    jedis.rpush(bfoo, bA);
    jedis.rpush(bfoo, bB);
    jedis.rpush(bfoo, bC);

    jedis.rpush(bdst, bfoo);
    jedis.rpush(bdst, bbar);

    byte[] belement = blocking(batchJedis.rpoplpush(bfoo, bdst));

    assertArrayEquals(bC, belement);

    List<byte[]> bsrcExpected = new ArrayList<>();
    bsrcExpected.add(bA);
    bsrcExpected.add(bB);

    List<byte[]> bdstExpected = new ArrayList<>();
    bdstExpected.add(bC);
    bdstExpected.add(bfoo);
    bdstExpected.add(bbar);

    assertByteArrayListEquals(bsrcExpected, jedis.lrange(bfoo, 0, 1000));
    assertByteArrayListEquals(bdstExpected, jedis.lrange(bdst, 0, 1000));
  }

  @Test
  public void lindex() {
    jedis.lpush("foo", "1");
    jedis.lpush("foo", "2");
    jedis.lpush("foo", "3");

    assertEquals("3", blocking(batchJedis.lindex("foo", 0)));
    assertEquals(null, blocking(batchJedis.lindex("foo", 100)));

    // Binary
    jedis.lpush(bfoo, b1);
    jedis.lpush(bfoo, b2);
    jedis.lpush(bfoo, b3);

    assertArrayEquals(b3, blocking(batchJedis.lindex(bfoo, 0)));
    assertEquals(null, blocking(batchJedis.lindex(bfoo, 100)));
  }

  @Test
  public void linsert() {
    long status = blocking(batchJedis.linsert("foo", LIST_POSITION.BEFORE, "bar", "car"));
    assertEquals(0, status);

    jedis.lpush("foo", "a");
    status = blocking(batchJedis.linsert("foo", LIST_POSITION.AFTER, "a", "b"));
    assertEquals(2, status);

    List<String> actual = jedis.lrange("foo", 0, 100);
    List<String> expected = new ArrayList<>();
    expected.add("a");
    expected.add("b");

    assertEquals(expected, actual);

    status = blocking(batchJedis.linsert("foo", LIST_POSITION.BEFORE, "bar", "car"));
    assertEquals(-1, status);

    // Binary
    long bstatus = blocking(batchJedis.linsert(bfoo, LIST_POSITION.BEFORE, bbar, bcar));
    assertEquals(0, bstatus);

    jedis.lpush(bfoo, bA);
    bstatus = blocking(batchJedis.linsert(bfoo, LIST_POSITION.AFTER, bA, bB));
    assertEquals(2, bstatus);

    List<byte[]> bactual = jedis.lrange(bfoo, 0, 100);
    List<byte[]> bexpected = new ArrayList<>();
    bexpected.add(bA);
    bexpected.add(bB);

    assertByteArrayListEquals(bexpected, bactual);

    bstatus = blocking(batchJedis.linsert(bfoo, LIST_POSITION.BEFORE, bbar, bcar));
    assertEquals(-1, bstatus);
  }

  @Test
  public void llen() {
    assertEquals(0, blocking(batchJedis.llen("foo")).intValue());
    jedis.lpush("foo", "bar");
    jedis.lpush("foo", "car");
    assertEquals(2, blocking(batchJedis.llen("foo")).intValue());

    // Binary
    assertEquals(0, blocking(batchJedis.llen(bfoo)).intValue());
    jedis.lpush(bfoo, bbar);
    jedis.lpush(bfoo, bcar);
    assertEquals(2, blocking(batchJedis.llen(bfoo)).intValue());
  }

  @Test
  public void llenNotOnList() throws InterruptedException {
    try {
      jedis.set("foo", "bar");
      MessageFuture<Long> fooFuture = batchJedis.llen("foo");
      reporter.flush();
      fooFuture.get();
      fail("ExecutionException expected");
    } catch (final ExecutionException e) {
    }

    // Binary
    try {
      jedis.set(bfoo, bbar);
      MessageFuture<Long> bfooFuture = batchJedis.llen(bfoo);
      reporter.flush();
      bfooFuture.get();
      fail("ExecutionException expected");
    } catch (final ExecutionException e) {
    }
  }

  @Test
  public void lpop() {
    jedis.rpush("foo", "a");
    jedis.rpush("foo", "b");
    jedis.rpush("foo", "c");

    String element = blocking(batchJedis.lpop("foo"));
    assertEquals("a", element);

    List<String> expected = new ArrayList<>();
    expected.add("b");
    expected.add("c");

    assertEquals(expected, jedis.lrange("foo", 0, 1000));
    blocking(batchJedis.lpop("foo"));
    blocking(batchJedis.lpop("foo"));

    element = blocking(batchJedis.lpop("foo"));
    assertNull(element);

    // Binary
    jedis.rpush(bfoo, bA);
    jedis.rpush(bfoo, bB);
    jedis.rpush(bfoo, bC);

    byte[] belement = blocking(batchJedis.lpop(bfoo));
    assertArrayEquals(bA, belement);

    List<byte[]> bexpected = new ArrayList<>();
    bexpected.add(bB);
    bexpected.add(bC);

    assertByteArrayListEquals(bexpected, jedis.lrange(bfoo, 0, 1000));
    blocking(batchJedis.lpop(bfoo));
    blocking(batchJedis.lpop(bfoo));

    belement = blocking(batchJedis.lpop(bfoo));
    assertNull(belement);
  }

  @Test
  public void lpush() {
    long size = blocking(batchJedis.lpush("foo", "bar"));
    assertEquals(1, size);
    size = blocking(batchJedis.lpush("foo", "foo"));
    assertEquals(2, size);
    size = blocking(batchJedis.lpush("foo", "bar", "foo"));
    assertEquals(4, size);

    // Binary
    long bsize = blocking(batchJedis.lpush(bfoo, bbar));
    assertEquals(1, bsize);
    bsize = blocking(batchJedis.lpush(bfoo, bfoo));
    assertEquals(2, bsize);
    bsize = blocking(batchJedis.lpush(bfoo, bbar, bfoo));
    assertEquals(4, bsize);
  }

  @Test
  public void lpushx() {
    long status = blocking(batchJedis.lpushx("foo", "bar"));
    assertEquals(0, status);

    jedis.lpush("foo", "a");
    status = blocking(batchJedis.lpushx("foo", "b"));
    assertEquals(2, status);

    // Binary
    long bstatus = blocking(batchJedis.lpushx(bfoo, bbar));
    assertEquals(0, bstatus);

    jedis.lpush(bfoo, bA);
    bstatus = blocking(batchJedis.lpushx(bfoo, bB));
    assertEquals(2, bstatus);
  }

  @Test
  public void lrange() {
    jedis.rpush("foo", "a");
    jedis.rpush("foo", "b");
    jedis.rpush("foo", "c");

    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");

    List<String> range = blocking(batchJedis.lrange("foo", 0, 2));
    assertEquals(expected, range);

    range = blocking(batchJedis.lrange("foo", 0, 20));
    assertEquals(expected, range);

    expected = new ArrayList<String>();
    expected.add("b");
    expected.add("c");

    range = blocking(batchJedis.lrange("foo", 1, 2));
    assertEquals(expected, range);

    expected = new ArrayList<String>();
    range = blocking(batchJedis.lrange("foo", 2, 1));
    assertEquals(expected, range);

    // Binary
    jedis.rpush(bfoo, bA);
    jedis.rpush(bfoo, bB);
    jedis.rpush(bfoo, bC);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);
    bexpected.add(bC);

    List<byte[]> brange = blocking(batchJedis.lrange(bfoo, 0, 2));
    assertByteArrayListEquals(bexpected, brange);

    brange = blocking(batchJedis.lrange(bfoo, 0, 20));
    assertByteArrayListEquals(bexpected, brange);

    bexpected = new ArrayList<byte[]>();
    bexpected.add(bB);
    bexpected.add(bC);

    brange = blocking(batchJedis.lrange(bfoo, 1, 2));
    assertByteArrayListEquals(bexpected, brange);

    bexpected = new ArrayList<byte[]>();
    brange = blocking(batchJedis.lrange(bfoo, 2, 1));
    assertByteArrayListEquals(bexpected, brange);
  }

  @Test
  public void lrem() {
    jedis.lpush("foo", "hello");
    jedis.lpush("foo", "hello");
    jedis.lpush("foo", "x");
    jedis.lpush("foo", "hello");
    jedis.lpush("foo", "c");
    jedis.lpush("foo", "b");
    jedis.lpush("foo", "a");

    long count = blocking(batchJedis.lrem("foo", -2, "hello"));

    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");
    expected.add("hello");
    expected.add("x");

    assertEquals(2, count);
    assertEquals(expected, jedis.lrange("foo", 0, 1000));
    assertEquals(0, blocking(batchJedis.lrem("bar", 100, "foo")).intValue());

    // Binary
    jedis.lpush(bfoo, bhello);
    jedis.lpush(bfoo, bhello);
    jedis.lpush(bfoo, bx);
    jedis.lpush(bfoo, bhello);
    jedis.lpush(bfoo, bC);
    jedis.lpush(bfoo, bB);
    jedis.lpush(bfoo, bA);

    long bcount = blocking(batchJedis.lrem(bfoo, -2, bhello));

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);
    bexpected.add(bC);
    bexpected.add(bhello);
    bexpected.add(bx);

    assertEquals(2, bcount);
    assertByteArrayListEquals(bexpected, jedis.lrange(bfoo, 0, 1000));
    assertEquals(0, blocking(batchJedis.lrem(bbar, 100, bfoo)).intValue());
  }

  @Test
  public void lset() {
    jedis.lpush("foo", "1");
    jedis.lpush("foo", "2");
    jedis.lpush("foo", "3");

    List<String> expected = new ArrayList<String>();
    expected.add("3");
    expected.add("bar");
    expected.add("1");

    String status = blocking(batchJedis.lset("foo", 1, "bar"));

    assertEquals("OK", status);
    assertEquals(expected, jedis.lrange("foo", 0, 100));

    // Binary
    jedis.lpush(bfoo, b1);
    jedis.lpush(bfoo, b2);
    jedis.lpush(bfoo, b3);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b3);
    bexpected.add(bbar);
    bexpected.add(b1);

    String bstatus = blocking(batchJedis.lset(bfoo, 1, bbar));

    assertEquals("OK", bstatus);
    assertByteArrayListEquals(bexpected, jedis.lrange(bfoo, 0, 100));
  }

  @Test
  public void ltrim() {
    jedis.lpush("foo", "1");
    jedis.lpush("foo", "2");
    jedis.lpush("foo", "3");
    String status = blocking(batchJedis.ltrim("foo", 0, 1));

    List<String> expected = new ArrayList<String>();
    expected.add("3");
    expected.add("2");

    assertEquals("OK", status);
    assertEquals(2, jedis.llen("foo").intValue());
    assertEquals(expected, jedis.lrange("foo", 0, 100));

    // Binary
    jedis.lpush(bfoo, b1);
    jedis.lpush(bfoo, b2);
    jedis.lpush(bfoo, b3);
    String bstatus = blocking(batchJedis.ltrim(bfoo, 0, 1));

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b3);
    bexpected.add(b2);

    assertEquals("OK", bstatus);
    assertEquals(2, jedis.llen(bfoo).intValue());
    assertByteArrayListEquals(bexpected, jedis.lrange(bfoo, 0, 100));
  }

  @Test
  public void rpop() {
    jedis.rpush("foo", "a");
    jedis.rpush("foo", "b");
    jedis.rpush("foo", "c");

    String element = blocking(batchJedis.rpop("foo"));
    assertEquals("c", element);

    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");

    assertEquals(expected, jedis.lrange("foo", 0, 1000));
    blocking(batchJedis.rpop("foo"));
    blocking(batchJedis.rpop("foo"));

    element = blocking(batchJedis.rpop("foo"));
    assertNull(element);

    // Binary
    jedis.rpush(bfoo, bA);
    jedis.rpush(bfoo, bB);
    jedis.rpush(bfoo, bC);

    byte[] belement = blocking(batchJedis.rpop(bfoo));
    assertArrayEquals(bC, belement);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);

    assertByteArrayListEquals(bexpected, jedis.lrange(bfoo, 0, 1000));
    blocking(batchJedis.rpop(bfoo));
    blocking(batchJedis.rpop(bfoo));

    belement = blocking(batchJedis.rpop(bfoo));
    assertNull(belement);
  }

  @Test
  public void rpush() {
    long size = blocking(batchJedis.rpush("foo", "bar"));
    assertEquals(1, size);
    size = blocking(batchJedis.rpush("foo", "foo"));
    assertEquals(2, size);
    size = blocking(batchJedis.rpush("foo", "bar", "foo"));
    assertEquals(4, size);

    // Binary
    long bsize = blocking(batchJedis.rpush(bfoo, bbar));
    assertEquals(1, bsize);
    bsize = blocking(batchJedis.rpush(bfoo, bfoo));
    assertEquals(2, bsize);
    bsize = blocking(batchJedis.rpush(bfoo, bbar, bfoo));
    assertEquals(4, bsize);
  }

  @Test
  public void rpushx() {
    long status = blocking(batchJedis.rpushx("foo", "bar"));
    assertEquals(0, status);

    jedis.lpush("foo", "a");
    status = blocking(batchJedis.rpushx("foo", "b"));
    assertEquals(2, status);

    // Binary
    long bstatus = blocking(batchJedis.rpushx(bfoo, bbar));
    assertEquals(0, bstatus);

    jedis.lpush(bfoo, bA);
    bstatus = blocking(batchJedis.rpushx(bfoo, bB));
    assertEquals(2, bstatus);
  }

  @Test
  public void brpoplpush() {
    (new Thread(new Runnable() {
      public void run() {
        try {
          Thread.sleep(100);
          Jedis j = createJedis();
          j.lpush("foo", "a");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    })).start();

    String element = jedis.brpoplpush("foo", "bar", 0);

    assertEquals("a", element);
    assertEquals(1, jedis.llen("bar").longValue());
    assertEquals("a", jedis.lrange("bar", 0, -1).get(0));

    (new Thread(new Runnable() {
      public void run() {
        try {
          Thread.sleep(100);
          Jedis j = createJedis();
          j.lpush("foo", "a");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    })).start();

    byte[] brpoplpush = jedis.brpoplpush("foo".getBytes(), "bar".getBytes(), 0);

    assertTrue(Arrays.equals("a".getBytes(), brpoplpush));
    assertEquals(1, jedis.llen("bar").longValue());
    assertEquals("a", jedis.lrange("bar", 0, -1).get(0));
  }
}
