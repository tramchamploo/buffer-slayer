package io.github.tramchamploo.bufferslayer;

import java.util.Map;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Pipeline;
import redis.clients.util.SafeEncoder;

/**
 * A redis command to be executed
 */
abstract class RedisCommand extends Message {

  final byte[] key;

  RedisCommand(byte[] key) {
    this.key = key;
  }

  /**
   * This implies how redis pipeline execute this command.
   *
   * @param pipeline pipeline to behave on.
   */
  protected abstract void apply(Pipeline pipeline);

  @Override
  public MessageKey asMessageKey() {
    return Message.SINGLE_KEY;
  }

  String keysString() {
    return SafeEncoder.encode(key);
  }

  // Commands start

  final static class Append extends RedisCommand {

    final byte[] value;

    Append(byte[] key, byte[] value) {
      super(key);
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.append(key, value);
    }

    @Override
    public String toString() {
      return "Append(" + new String(key) + ", " + SafeEncoder.encode(value) + ")";
    }
  }

  final static class Decr extends RedisCommand {

    Decr(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.decr(key);
    }

    @Override
    public String toString() {
      return "Decr(" + keysString() + ")";
    }
  }

  final static class DecrBy extends RedisCommand {

    final long value;

    DecrBy(byte[] key, long value) {
      super(key);
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.decrBy(key, value);
    }

    @Override
    public String toString() {
      return "DecrBy(" + keysString() + ", " +  + value + ")";
    }
  }

  final static class Echo extends RedisCommand {

    Echo(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.echo(key);
    }

    @Override
    public String toString() {
      return "Echo(" + keysString() + ")";
    }
  }

  final static class Expire extends RedisCommand {

    final int seconds;

    Expire(byte[] key, int seconds) {
      super(key);
      this.seconds = seconds;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.expire(key, seconds);
    }

    @Override
    public String toString() {
      return "Expire(" + keysString() + ", " + seconds + ")";
    }
  }

  final static class ExpireAt extends RedisCommand {

    final long unixTime;

    ExpireAt(byte[] key, long unixTime) {
      super(key);
      this.unixTime = unixTime;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.expireAt(key, unixTime);
    }

    @Override
    public String toString() {
      return "ExpireAt(" + keysString() + ", " + unixTime + ")";
    }
  }

  final static class Get extends RedisCommand {

    Get(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.get(key);
    }

    @Override
    public String toString() {
      return "Get(" + keysString() + ")";
    }
  }

  final static class GetBit extends RedisCommand {

    long offset;

    GetBit(byte[] key, long offset) {
      super(key);
      this.offset = offset;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.getbit(key, offset);
    }

    @Override
    public String toString() {
      return "GetBit(" + keysString() + ", " + offset + ")";
    }
  }

  final static class BitPos extends RedisCommand {

    boolean value;
    redis.clients.jedis.BitPosParams params;

    BitPos(byte[] key, boolean value, redis.clients.jedis.BitPosParams params) {
      super(key);
      this.value = value;
      this.params = params;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.bitpos(key, value, params);
    }

    @Override
    public String toString() {
      return "BitPos(" + keysString() + ", " + value + ", "
          + BuilderFactory.STRING_LIST.build(params.getParams()).toString() + ")";
    }
  }

  final static class GetRange extends RedisCommand {

    String keyString;
    long startOffset;
    long endOffset;

    GetRange(String key, long startOffset, long endOffset) {
      super(null);
      this.keyString = key;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.getrange(keyString, startOffset, endOffset);
    }

    @Override
    public String toString() {
      return "GetRange(" + keysString() + ", " + startOffset + ", " + endOffset + ")";
    }

    String keysString() {
      return keyString;
    }
  }

  final static class Set extends RedisCommand {

    byte[] value;
    byte[] nxxx;
    byte[] expx;
    long time;

    Set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) {
      super(key);
      this.value = value;
      this.nxxx = nxxx;
      this.expx = expx;
      this.time = time;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      if (expx != null) {
        pipeline.set(key, value, nxxx, expx, (int) time);
      } else if (nxxx != null) {
        pipeline.set(key, value, nxxx);
      } else {
        pipeline.set(key, value);
      }
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("Set(")
          .append(keysString()).append(", ")
          .append(SafeEncoder.encode(value));
      if (nxxx != null) {
        builder.append(", ").append(SafeEncoder.encode(nxxx));
      }
      if (expx != null) {
        builder.append(", ").append(SafeEncoder.encode(expx))
            .append(", ").append(time);
      }
      return builder.append(")").toString();
    }
  }

  final static class GetSet extends RedisCommand {

    byte[] value;

    GetSet(byte[] key, byte[] value) {
      super(key);
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.getSet(key, value);
    }

    @Override
    public String toString() {
      return "GetSet(" + keysString() + ", " + SafeEncoder.encode(value) + ")";
    }
  }

  final static class HDel extends RedisCommand {

    final byte[][] field;

    HDel(byte[] key, byte[]... field) {
      super(key);
      this.field = field;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hdel(key, field);
    }

    @Override
    public String toString() {
        return "HDel(" + keysString() + ", " + RedisCommandUtil.toString(field) + ")";
    }
  }

  final static class HExists extends RedisCommand {

    final byte[] field;

    HExists(byte[] key, byte[] field) {
      super(key);
      this.field = field;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hexists(key, field);
    }

    @Override
    public String toString() {
      return "HExists(" + keysString() + ", " + SafeEncoder.encode(field) + ")";
    }
  }

  final static class HGet extends RedisCommand {

    final byte[] field;

    HGet(byte[] key, byte[] field) {
      super(key);
      this.field = field;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hget(key, field);
    }

    @Override
    public String toString() {
      return "HGet(" + keysString() + ", " + SafeEncoder.encode(field) + ")";
    }
  }

  final static class HGetAll extends RedisCommand {

    HGetAll(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hgetAll(key);
    }

    @Override
    public String toString() {
      return "HGetAll(" + keysString() + ")";
    }
  }

  final static class HIncrBy extends RedisCommand {

    final byte[] field;
    final long value;

    HIncrBy(byte[] key, byte[] field, long value) {
      super(key);
      this.field = field;
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hincrBy(key, field, value);
    }

    @Override
    public String toString() {
      return "HIncrBy(" + keysString() + ", " + SafeEncoder.encode(field) + ", " + value + ")";
    }
  }

  final static class HKeys extends RedisCommand {

    HKeys(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hkeys(key);
    }

    @Override
    public String toString() {
      return "HKeys(" + keysString() + ")";
    }
  }

  final static class HLen extends RedisCommand {

    HLen(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hlen(key);
    }

    @Override
    public String toString() {
      return "HLen(" + keysString() + ")";
    }
  }

  final static class HMGet extends RedisCommand {

    final byte[][] field;

    HMGet(byte[] key, byte[]... field) {
      super(key);
      this.field = field;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hmget(key, field);
    }

    @Override
    public String toString() {
      return "HMGet(" + keysString() + ", " + RedisCommandUtil.toString(field) + ")";
    }
  }

  final static class HMSet extends RedisCommand {

    final Map<byte[], byte[]> hash;

    HMSet(byte[] key, Map<byte[], byte[]> hash) {
      super(key);
      this.hash = hash;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hmset(key, hash);
    }

    @Override
    public String toString() {
      return "HMSet(" + keysString() + ", " + RedisCommandUtil.toString(hash) + ")";
    }
  }

  final static class HSet extends RedisCommand {

    final byte[] field;
    final byte[] value;

    HSet(byte[] key, byte[] field, byte[] value) {
      super(key);
      this.field = field;
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hset(key, field, value);
    }

    @Override
    public String toString() {
      return "HSet(" + keysString() + ", " + SafeEncoder.encode(field) + ", "
          + SafeEncoder.encode(value) + ")";
    }
  }

  final static class HSetNX extends RedisCommand {

    final byte[] field;
    final byte[] value;

    HSetNX(byte[] key, byte[] field, byte[] value) {
      super(key);
      this.field = field;
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hsetnx(key, field, value);
    }

    @Override
    public String toString() {
      return "HSetNX(" + keysString() + ", " + SafeEncoder.encode(field) + ", "
          + SafeEncoder.encode(value) + ")";
    }
  }

  final static class HVals extends RedisCommand {

    HVals(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.hvals(key);
    }

    @Override
    public String toString() {
      return "HVals(" + keysString() + ")";
    }
  }

  final static class Incr extends RedisCommand {

    Incr(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.incr(key);
    }

    @Override
    public String toString() {
      return "Incr(" + keysString() + ")";
    }
  }

  final static class IncrBy extends RedisCommand {

    final long value;

    IncrBy(byte[] key, long value) {
      super(key);
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.incrBy(key, value);
    }

    @Override
    public String toString() {
      return "IncrBy(" + keysString() + ", " + value + ")";
    }
  }

  final static class Rename extends RedisCommand {

    final byte[] newKey;

    Rename(byte[] oldKey, byte[] newKey) {
      super(oldKey);
      this.newKey = newKey;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.rename(key, newKey);
    }

    @Override
    public String toString() {
      return "Rename(" + keysString() + ", " + SafeEncoder.encode(newKey) + ")";
    }
  }

  final static class RenameNX extends RedisCommand {

    final byte[] newKey;

    RenameNX(byte[] oldKey, byte[] newKey) {
      super(oldKey);
      this.newKey = newKey;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.renamenx(key, newKey);
    }

    @Override
    public String toString() {
      return "RenameNX(" + keysString() + ", " + SafeEncoder.encode(newKey) + ")";
    }
  }

  final static class RPopLpush extends RedisCommand {

    final byte[] dstkey;

    RPopLpush(byte[] srckey, byte[] dstkey) {
      super(srckey);
      this.dstkey = dstkey;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.rpoplpush(key, dstkey);
    }

    @Override
    public String toString() {
      return "RPopLpush(" + keysString() + ", " + SafeEncoder.encode(dstkey) + ")";
    }
  }

  final static class LIndex extends RedisCommand {

    final long index;

    LIndex(byte[] key, long index) {
      super(key);
      this.index = index;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.lindex(key, index);
    }

    @Override
    public String toString() {
      return "LIndex(" + keysString() + ", " + index + ")";
    }
  }

  final static class LInsert extends RedisCommand {

    final LIST_POSITION where;
    final byte[] pivot;
    final byte[] value;

    LInsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
      super(key);
      this.where = where;
      this.pivot = pivot;
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.linsert(key, where, pivot, value);
    }

    @Override
    public String toString() {
      return "LInsert(" + keysString() + ", " + where + ", " +
          SafeEncoder.encode(pivot) + ", " + SafeEncoder.encode(value) + ")";
    }
  }

  final static class LLen extends RedisCommand {

    LLen(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.llen(key);
    }

    @Override
    public String toString() {
      return "LLen(" + keysString() + ")";
    }
  }

  final static class LPop extends RedisCommand {

    LPop(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.lpop(key);
    }

    @Override
    public String toString() {
      return "LPop(" + keysString() + ")";
    }
  }

  final static class LPush extends RedisCommand {

    final byte[][] string;

    LPush(byte[] key, byte[]... string) {
      super(key);
      this.string = string;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.lpush(key, string);
    }

    @Override
    public String toString() {
      return "LPush(" + keysString() + ", " + RedisCommandUtil.toString(string) + ")";
    }
  }

  final static class LPushX extends RedisCommand {

    final byte[][] string;

    LPushX(byte[] key, byte[]... string) {
      super(key);
      this.string = string;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.lpushx(key, string);
    }

    @Override
    public String toString() {
      return "LPushX(" + keysString() + ", " + RedisCommandUtil.toString(string) + ")";
    }
  }

  final static class LRange extends RedisCommand {

    final long start;
    final long end;

    LRange(byte[] key, long start, long end) {
      super(key);
      this.start = start;
      this.end = end;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.lrange(key, start, end);
    }

    @Override
    public String toString() {
      return "LRange(" + keysString() + ", " + start + ", " + end + ")";
    }
  }

  final static class LRem extends RedisCommand {

    final long count;
    final byte[] value;

    LRem(byte[] key, long count, byte[] value) {
      super(key);
      this.count = count;
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.lrem(key, count, value);
    }

    @Override
    public String toString() {
      return "LRem(" + keysString() + ", " + count + ", " + SafeEncoder.encode(value) + ")";
    }
  }

  final static class LSet extends RedisCommand {

    final long index;
    final byte[] value;

    LSet(byte[] key, long index, byte[] value) {
      super(key);
      this.index = index;
      this.value = value;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.lset(key, index, value);
    }

    @Override
    public String toString() {
      return "LSet(" + keysString() + ", " + index + ", " + SafeEncoder.encode(value) + ")";
    }
  }

  final static class LTrim extends RedisCommand {

    final long start;
    final long end;

    LTrim(byte[] key, long start, long end) {
      super(key);
      this.start = start;
      this.end = end;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.ltrim(key, start, end);
    }

    @Override
    public String toString() {
      return "LTrim(" + keysString() + ", " + start + ", " + end + ")";
    }
  }

  final static class Move extends RedisCommand {

    final int dbIndex;

    Move(byte[] key, int dbIndex) {
      super(key);
      this.dbIndex = dbIndex;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.move(key, dbIndex);
    }

    @Override
    public String toString() {
      return "Move(" + keysString() + ", " + dbIndex + ")";
    }
  }

  final static class Persist extends RedisCommand {

    Persist(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.persist(key);
    }

    @Override
    public String toString() {
      return "Persist(" + keysString() + ")";
    }
  }

  final static class RPop extends RedisCommand {

    RPop(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.rpop(key);
    }

    @Override
    public String toString() {
      return "RPop(" + keysString() + ")";
    }
  }

  final static class RPush extends RedisCommand {

    final byte[][] string;

    RPush(byte[] key, byte[]... string) {
      super(key);
      this.string = string;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.rpush(key, string);
    }

    @Override
    public String toString() {
      return "RPush(" + keysString() + ", " + RedisCommandUtil.toString(string) + ")";
    }
  }

  final static class RPushX extends RedisCommand {

    final byte[][] string;

    RPushX(byte[] key, byte[]... string) {
      super(key);
      this.string = string;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.rpushx(key, string);
    }

    @Override
    public String toString() {
      return "RPushX(" + keysString() + ", " + RedisCommandUtil.toString(string) + ")";
    }
  }

  final static class SAdd extends RedisCommand {

    final byte[][] members;

    SAdd(byte[] key, byte[]... members) {
      super(key);
      this.members = members;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.sadd(key, members);
    }

    @Override
    public String toString() {
      return "SAdd(" + keysString() + ", " + RedisCommandUtil.toString(members) + ")";
    }
  }

  final static class SMembers extends RedisCommand {

    SMembers(byte[] key) {
      super(key);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.smembers(key);
    }

    @Override
    public String toString() {
      return "SMembers(" + keysString() + ")";
    }
  }
}
