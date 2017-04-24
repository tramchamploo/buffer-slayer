package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.ResponseUtil.transformResponse;

import com.google.common.annotations.VisibleForTesting;
import io.github.tramchamploo.bufferslayer.MultiKeyRedisCommand.Blpop;
import io.github.tramchamploo.bufferslayer.MultiKeyRedisCommand.Brpop;
import io.github.tramchamploo.bufferslayer.MultiKeyRedisCommand.BrpopLpush;
import io.github.tramchamploo.bufferslayer.MultiKeyRedisCommand.Del;
import io.github.tramchamploo.bufferslayer.MultiKeyRedisCommand.Exists;
import io.github.tramchamploo.bufferslayer.MultiKeyRedisCommand.MGet;
import io.github.tramchamploo.bufferslayer.MultiKeyRedisCommand.MSet;
import io.github.tramchamploo.bufferslayer.MultiKeyRedisCommand.MSetNX;
import io.github.tramchamploo.bufferslayer.RedisCommand.Append;
import io.github.tramchamploo.bufferslayer.RedisCommand.BitPos;
import io.github.tramchamploo.bufferslayer.RedisCommand.Decr;
import io.github.tramchamploo.bufferslayer.RedisCommand.DecrBy;
import io.github.tramchamploo.bufferslayer.RedisCommand.Echo;
import io.github.tramchamploo.bufferslayer.RedisCommand.Expire;
import io.github.tramchamploo.bufferslayer.RedisCommand.ExpireAt;
import io.github.tramchamploo.bufferslayer.RedisCommand.Get;
import io.github.tramchamploo.bufferslayer.RedisCommand.GetBit;
import io.github.tramchamploo.bufferslayer.RedisCommand.GetRange;
import io.github.tramchamploo.bufferslayer.RedisCommand.GetSet;
import io.github.tramchamploo.bufferslayer.RedisCommand.HDel;
import io.github.tramchamploo.bufferslayer.RedisCommand.HExists;
import io.github.tramchamploo.bufferslayer.RedisCommand.HGet;
import io.github.tramchamploo.bufferslayer.RedisCommand.HGetAll;
import io.github.tramchamploo.bufferslayer.RedisCommand.HIncrBy;
import io.github.tramchamploo.bufferslayer.RedisCommand.HKeys;
import io.github.tramchamploo.bufferslayer.RedisCommand.HLen;
import io.github.tramchamploo.bufferslayer.RedisCommand.HMGet;
import io.github.tramchamploo.bufferslayer.RedisCommand.HMSet;
import io.github.tramchamploo.bufferslayer.RedisCommand.HSet;
import io.github.tramchamploo.bufferslayer.RedisCommand.HSetNX;
import io.github.tramchamploo.bufferslayer.RedisCommand.HVals;
import io.github.tramchamploo.bufferslayer.RedisCommand.Incr;
import io.github.tramchamploo.bufferslayer.RedisCommand.IncrBy;
import io.github.tramchamploo.bufferslayer.RedisCommand.LIndex;
import io.github.tramchamploo.bufferslayer.RedisCommand.LInsert;
import io.github.tramchamploo.bufferslayer.RedisCommand.LLen;
import io.github.tramchamploo.bufferslayer.RedisCommand.LPop;
import io.github.tramchamploo.bufferslayer.RedisCommand.LPush;
import io.github.tramchamploo.bufferslayer.RedisCommand.LPushX;
import io.github.tramchamploo.bufferslayer.RedisCommand.LRange;
import io.github.tramchamploo.bufferslayer.RedisCommand.LRem;
import io.github.tramchamploo.bufferslayer.RedisCommand.LSet;
import io.github.tramchamploo.bufferslayer.RedisCommand.LTrim;
import io.github.tramchamploo.bufferslayer.RedisCommand.Move;
import io.github.tramchamploo.bufferslayer.RedisCommand.Persist;
import io.github.tramchamploo.bufferslayer.RedisCommand.RPop;
import io.github.tramchamploo.bufferslayer.RedisCommand.RPopLpush;
import io.github.tramchamploo.bufferslayer.RedisCommand.RPush;
import io.github.tramchamploo.bufferslayer.RedisCommand.RPushX;
import io.github.tramchamploo.bufferslayer.RedisCommand.Rename;
import io.github.tramchamploo.bufferslayer.RedisCommand.RenameNX;
import io.github.tramchamploo.bufferslayer.RedisCommand.SAdd;
import io.github.tramchamploo.bufferslayer.RedisCommand.SMembers;
import io.github.tramchamploo.bufferslayer.internal.MessageFuture;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Client;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster.Reset;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import redis.clients.util.Pool;
import redis.clients.util.SafeEncoder;
import redis.clients.util.Slowlog;

/**
 * {@link Jedis} that execute commands in pipeline
 */
@SuppressWarnings("unchecked")
public class BatchJedis {

  private final JedisPool jedisPool;
  private final Reporter<RedisCommand, ?> reporter;

  @VisibleForTesting
  BatchJedis(JedisPool jedisPool, Reporter<RedisCommand, ?> reporter) {
    this.jedisPool = jedisPool;
    this.reporter = reporter;
  }

  public BatchJedis(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
    this.reporter = AsyncReporter.builder(new JedisSender(jedisPool)).build();
  }

  public MessageFuture<String> set(String key, String value) {
    return (MessageFuture<String>) reporter.report(
        new RedisCommand.Set(SafeEncoder.encode(key), SafeEncoder.encode(value), null, null, 0L));
  }

  public MessageFuture<String> set(String key, String value, String nxxx, String expx, long time) {
    return (MessageFuture<String>) reporter.report(
        new RedisCommand.Set(SafeEncoder.encode(key), SafeEncoder.encode(value), SafeEncoder.encode(nxxx), SafeEncoder.encode(expx), time));
  }

  public MessageFuture<String> get(String key) {
    return transformResponse(
        reporter.report(new Get(SafeEncoder.encode(key))), BuilderFactory.STRING);
  }

  public MessageFuture<Long> exists(String... keys) {
    return (MessageFuture<Long>) reporter.report(new Exists(SafeEncoder.encodeMany(keys)));
  }

  public MessageFuture<Boolean> exists(String key) {
    return transformResponse(
        reporter.report(new Exists(SafeEncoder.encode(key))), BuilderFactory.BOOLEAN);
  }

  public MessageFuture<Long> del(String... keys) {
    return (MessageFuture<Long>) reporter.report(new Del(SafeEncoder.encodeMany(keys)));
  }

  public MessageFuture<Long> del(String key) {
    return (MessageFuture<Long>) reporter.report(new Del(SafeEncoder.encode(key)));
  }

  public String type(String key) {
    return null; // TODO
  }

  public Set<String> keys(String pattern) {
    return null; // TODO
  }

  public String randomKey() {
    return null; // TODO
  }

  public MessageFuture<String> rename(String oldkey, String newkey) {
    return (MessageFuture<String>) reporter.report(
        new Rename(SafeEncoder.encode(oldkey), SafeEncoder.encode(newkey)));
  }

  public MessageFuture<Long> renamenx(String oldkey, String newkey) {
    return (MessageFuture<Long>) reporter.report(
        new RenameNX(SafeEncoder.encode(oldkey), SafeEncoder.encode(newkey)));
  }

  public MessageFuture<Long> expire(String key, int seconds) {
    return (MessageFuture<Long>) reporter.report(new Expire(SafeEncoder.encode(key), seconds));
  }

  public MessageFuture<Long> expireAt(String key, long unixTime) {
    return (MessageFuture<Long>) reporter.report(new ExpireAt(SafeEncoder.encode(key), unixTime));
  }

  public Long ttl(String key) {
    return null; // TODO
  }

  public MessageFuture<Long> move(String key, int dbIndex) {
    return (MessageFuture<Long>) reporter.report(new Move(SafeEncoder.encode(key), dbIndex));
  }

  public MessageFuture<String> getSet(String key, String value) {
    return transformResponse(reporter.report(
        new GetSet(SafeEncoder.encode(key), SafeEncoder.encode(value))), BuilderFactory.STRING);
  }

  public MessageFuture<List<String>> mget(String... keys) {
    return transformResponse(reporter.report(
        new MGet(SafeEncoder.encodeMany(keys))), BuilderFactory.STRING_LIST);
  }

  public Long setnx(String key, String value) {
    return null; // TODO
  }

  public String setex(String key, int seconds, String value) {
    return null; // TODO
  }

  public MessageFuture<String> mset(String... keysvalues) {
    return (MessageFuture<String>) reporter.report(new MSet(SafeEncoder.encodeMany(keysvalues)));
  }

  public MessageFuture<Long> msetnx(String... keysvalues) {
    return (MessageFuture<Long>) reporter.report(new MSetNX(SafeEncoder.encodeMany(keysvalues)));
  }

  public MessageFuture<Long> decrBy(String key, long integer) {
    return (MessageFuture<Long>) reporter.report(new DecrBy(SafeEncoder.encode(key), integer));
  }

  public MessageFuture<Long> decr(String key) {
    return (MessageFuture<Long>) reporter.report(new Decr(SafeEncoder.encode(key)));
  }

  public MessageFuture<Long> incrBy(String key, long integer) {
    return (MessageFuture<Long>) reporter.report(
        new IncrBy(SafeEncoder.encode(key), integer));
  }

  public Double incrByFloat(String key, double value) {
    return null; // TODO
  }

  public MessageFuture<Long> incr(String key) {
    return (MessageFuture<Long>) reporter.report(new Incr(SafeEncoder.encode(key)));
  }

  public MessageFuture<Long> append(String key, String value) {
    return (MessageFuture<Long>) reporter.report(
        new Append(SafeEncoder.encode(key), SafeEncoder.encode(value)));
  }

  public String substr(String key, int start, int end) {
    return null; // TODO
  }

  public MessageFuture<Long> hset(String key, String field, String value) {
    return hset(SafeEncoder.encode(key), SafeEncoder.encode(field), SafeEncoder.encode(value));
  }

  public MessageFuture<String> hget(String key, String field) {
    return transformResponse(reporter.report(
        new HGet(SafeEncoder.encode(key), SafeEncoder.encode(field))), BuilderFactory.STRING);
  }

  public MessageFuture<Long> hsetnx(String key, String field, String value) {
    return hsetnx(SafeEncoder.encode(key), SafeEncoder.encode(field), SafeEncoder.encode(value));
  }

  public MessageFuture<String> hmset(String key, Map<String, String> hash) {
    final Map<byte[], byte[]> bhash = new HashMap<>(hash.size());
    for (final Entry<String, String> entry : hash.entrySet()) {
      bhash.put(SafeEncoder.encode(entry.getKey()), SafeEncoder.encode(entry.getValue()));
    }
    return hmset(SafeEncoder.encode(key), bhash);
  }

  public MessageFuture<List<String>> hmget(String key, String... fields) {
    return transformResponse(reporter.report(
        new HMGet(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields))), BuilderFactory.STRING_LIST);
  }

  public MessageFuture<Long> hincrBy(String key, String field, long value) {
    return (MessageFuture<Long>) reporter.report(
        new HIncrBy(SafeEncoder.encode(key), SafeEncoder.encode(field), value));
  }

  public Double hincrByFloat(String key, String field, double value) {
    return null; // TODO
  }

  public MessageFuture<Boolean> hexists(String key, String field) {
    return (MessageFuture<Boolean>) reporter.report(
        new HExists(SafeEncoder.encode(key), SafeEncoder.encode(field)));
  }

  public MessageFuture<Long> hdel(String key, String... fields) {
    return (MessageFuture<Long>) reporter.report(
        new HDel(SafeEncoder.encode(key), SafeEncoder.encodeMany(fields)));
  }

  public MessageFuture<Long> hlen(String key) {
    return (MessageFuture<Long>) reporter.report(new HLen(SafeEncoder.encode(key)));
  }

  public MessageFuture<Set<String>> hkeys(String key) {
    return transformResponse(
        reporter.report(new HKeys(SafeEncoder.encode(key))), BuilderFactory.STRING_SET);
  }

  public MessageFuture<List<String>> hvals(String key) {
    return transformResponse(reporter.report(new HVals(SafeEncoder.encode(key))),
        BuilderFactory.STRING_LIST);
  }

  public MessageFuture<Map<String, String>> hgetAll(String key) {
    return transformResponse(
        reporter.report(new HGetAll(SafeEncoder.encode(key))), BuilderFactory.STRING_MAP);
  }

  public MessageFuture<Long> rpush(String key, String... strings) {
    return (MessageFuture<Long>) reporter.report(
        new RPush(SafeEncoder.encode(key), SafeEncoder.encodeMany(strings)));
  }

  public MessageFuture<Long> lpush(String key, String... strings) {
    return (MessageFuture<Long>) reporter.report(
        new LPush(SafeEncoder.encode(key), SafeEncoder.encodeMany(strings)));
  }

  public MessageFuture<Long> llen(String key) {
    return (MessageFuture<Long>) reporter.report(new LLen(SafeEncoder.encode(key)));
  }

  public MessageFuture<List<String>> lrange(String key, long start, long end) {
    return ResponseUtil.transformResponse(reporter.report(
        new LRange(SafeEncoder.encode(key), start, end)), BuilderFactory.STRING_LIST);
  }

  public MessageFuture<String> ltrim(String key, long start, long end) {
    return (MessageFuture<String>) reporter.report(
        new LTrim(SafeEncoder.encode(key), start, end));
  }

  public MessageFuture<String> lindex(String key, long index) {
    return transformResponse(
        reporter.report(new LIndex(SafeEncoder.encode(key), index)), BuilderFactory.STRING);
  }

  public MessageFuture<String> lset(String key, long index, String value) {
    return (MessageFuture<String>) reporter.report(
        new LSet(SafeEncoder.encode(key), index, SafeEncoder.encode(value)));
  }

  public MessageFuture<Long> lrem(String key, long count, String value) {
    return (MessageFuture<Long>) reporter.report(
        new LRem(SafeEncoder.encode(key), count, SafeEncoder.encode(value)));
  }

  public MessageFuture<String> lpop(String key) {
    return transformResponse(reporter.report(new LPop(SafeEncoder.encode(key))), BuilderFactory.STRING);
  }

  public MessageFuture<String> rpop(String key) {
    return transformResponse(reporter.report(new RPop(SafeEncoder.encode(key))),
        BuilderFactory.STRING);
  }

  public MessageFuture<String> rpoplpush(String srckey, String dstkey) {
    return transformResponse(reporter.report(
        new RPopLpush(SafeEncoder.encode(srckey), SafeEncoder.encode(dstkey))), BuilderFactory.STRING);
  }

  public MessageFuture<Long> sadd(String key, String... members) {
    return (MessageFuture<Long>) reporter.report(
        new SAdd(SafeEncoder.encode(key), SafeEncoder.encodeMany(members)));
  }

  public MessageFuture<Set<String>> smembers(String key) {
    return transformResponse(reporter.report(new SMembers(SafeEncoder.encode(key))), BuilderFactory.STRING_SET);
  }

  public Long srem(String key, String... members) {
    return null; // TODO
  }

  public String spop(String key) {
    return null; // TODO
  }

  public Set<String> spop(String key, long count) {
    return null; // TODO
  }

  public Long smove(String srckey, String dstkey, String member) {
    return null; // TODO
  }

  public Long scard(String key) {
    return null; // TODO
  }

  public Boolean sismember(String key, String member) {
    return null; // TODO
  }

  public Set<String> sinter(String... keys) {
    return null; // TODO
  }

  public Long sinterstore(String dstkey, String... keys) {
    return null; // TODO
  }

  public Set<String> sunion(String... keys) {
    return null; // TODO
  }

  public Long sunionstore(String dstkey, String... keys) {
    return null; // TODO
  }

  public Set<String> sdiff(String... keys) {
    return null; // TODO
  }

  public Long sdiffstore(String dstkey, String... keys) {
    return null; // TODO
  }

  public String srandmember(String key) {
    return null; // TODO
  }

  public List<String> srandmember(String key, int count) {
    return null; // TODO
  }

  public Long zadd(String key, double score, String member) {
    return null; // TODO
  }

  public Long zadd(String key, double score, String member,
      ZAddParams params) {
    return null; // TODO
  }

  public Long zadd(String key, Map<String, Double> scoreMembers) {
    return null; // TODO
  }

  public Long zadd(String key, Map<String, Double> scoreMembers,
      ZAddParams params) {
    return null; // TODO
  }

  public Set<String> zrange(String key, long start, long end) {
    return null; // TODO
  }

  public Long zrem(String key, String... members) {
    return null; // TODO
  }

  public Double zincrby(String key, double score, String member) {
    return null; // TODO
  }

  public Double zincrby(String key, double score, String member,
      ZIncrByParams params) {
    return null; // TODO
  }

  public Long zrank(String key, String member) {
    return null; // TODO
  }

  public Long zrevrank(String key, String member) {
    return null; // TODO
  }

  public Set<String> zrevrange(String key, long start, long end) {
    return null; // TODO
  }

  public Set<Tuple> zrangeWithScores(String key, long start, long end) {
    return null; // TODO
  }

  public Set<Tuple> zrevrangeWithScores(String key, long start,
      long end) {
    return null; // TODO
  }

  public Long zcard(String key) {
    return null; // TODO
  }

  public Double zscore(String key, String member) {
    return null; // TODO
  }

  public String watch(String... keys) {
    return null; // TODO
  }

  public List<String> sort(String key) {
    return null; // TODO
  }

  public List<String> sort(String key, SortingParams sortingParameters) {
    return null; // TODO
  }

  public MessageFuture<List<String>> blpop(int timeout, String... keys) {
    return (MessageFuture<List<String>>) reporter.report(
        new Blpop(timeout, SafeEncoder.encodeMany(keys)));
  }

  public MessageFuture<List<String>> blpop(String... args) {
    return (MessageFuture<List<String>>) reporter.report(
        new Blpop(0, SafeEncoder.encodeMany(args)));
  }

  public MessageFuture<List<String>> brpop(String... args) {
    return (MessageFuture<List<String>>) reporter.report(
        new Brpop(0, SafeEncoder.encodeMany(args)));
  }

  @Deprecated
  public MessageFuture<List<String>> blpop(String arg) {
    return (MessageFuture<List<String>>) reporter.report(
        new Blpop(0, SafeEncoder.encode(arg)));
  }

  @Deprecated
  public MessageFuture<List<String>> brpop(String arg) {
    return (MessageFuture<List<String>>) reporter.report(
        new Brpop(0, SafeEncoder.encode(arg)));
  }

  public Long sort(String key, SortingParams sortingParameters,
      String dstkey) {
    return null; // TODO
  }

  public Long sort(String key, String dstkey) {
    return null; // TODO
  }

  public MessageFuture<List<String>> brpop(int timeout, String... keys) {
    return (MessageFuture<List<String>>) reporter.report(
        new Brpop(timeout, SafeEncoder.encodeMany(keys)));
  }

  public Long zcount(String key, double min, double max) {
    return null; // TODO
  }

  public Long zcount(String key, String min, String max) {
    return null; // TODO
  }

  public Set<String> zrangeByScore(String key, double min, double max) {
    return null; // TODO
  }

  public Set<String> zrangeByScore(String key, String min, String max) {
    return null; // TODO
  }

  public Set<String> zrangeByScore(String key, double min, double max, int offset,
      int count) {
    return null; // TODO
  }

  public Set<String> zrangeByScore(String key, String min, String max, int offset,
      int count) {
    return null; // TODO
  }

  public Set<Tuple> zrangeByScoreWithScores(String key, double min,
      double max) {
    return null; // TODO
  }

  public Set<Tuple> zrangeByScoreWithScores(String key,
      String min, String max) {
    return null; // TODO
  }

  public Set<Tuple> zrangeByScoreWithScores(String key, double min,
      double max, int offset, int count) {
    return null; // TODO
  }

  public Set<Tuple> zrangeByScoreWithScores(String key,
      String min, String max, int offset, int count) {
    return null; // TODO
  }

  public Set<String> zrevrangeByScore(String key, double max, double min) {
    return null; // TODO
  }

  public Set<String> zrevrangeByScore(String key, String max, String min) {
    return null; // TODO
  }

  public Set<String> zrevrangeByScore(String key, double max, double min, int offset,
      int count) {
    return null; // TODO
  }

  public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
      double min) {
    return null; // TODO
  }

  public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
      double min, int offset, int count) {
    return null; // TODO
  }

  public Set<Tuple> zrevrangeByScoreWithScores(String key,
      String max, String min, int offset, int count) {
    return null; // TODO
  }

  public Set<String> zrevrangeByScore(String key, String max, String min, int offset,
      int count) {
    return null; // TODO
  }

  public Set<Tuple> zrevrangeByScoreWithScores(String key,
      String max, String min) {
    return null; // TODO
  }

  public Long zremrangeByRank(String key, long start, long end) {
    return null; // TODO
  }

  public Long zremrangeByScore(String key, double start, double end) {
    return null; // TODO
  }

  public Long zremrangeByScore(String key, String start, String end) {
    return null; // TODO
  }

  public Long zunionstore(String dstkey, String... sets) {
    return null; // TODO
  }

  public Long zunionstore(String dstkey, ZParams params, String... sets) {
    return null; // TODO
  }

  public Long zinterstore(String dstkey, String... sets) {
    return null; // TODO
  }

  public Long zinterstore(String dstkey, ZParams params, String... sets) {
    return null; // TODO
  }

  public Long zlexcount(String key, String min, String max) {
    return null; // TODO
  }

  public Set<String> zrangeByLex(String key, String min, String max) {
    return null; // TODO
  }

  public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
    return null; // TODO
  }

  public Set<String> zrevrangeByLex(String key, String max, String min) {
    return null; // TODO
  }

  public Set<String> zrevrangeByLex(String key, String max, String min, int offset,
      int count) {
    return null; // TODO
  }

  public Long zremrangeByLex(String key, String min, String max) {
    return null; // TODO
  }

  public Long strlen(String key) {
    return null; // TODO
  }

  public MessageFuture<Long> lpushx(String key, String... string) {
    return (MessageFuture<Long>) reporter.report(
        new LPushX(SafeEncoder.encode(key), SafeEncoder.encodeMany(string)));
  }

  public MessageFuture<Long> persist(String key) {
    return (MessageFuture<Long>) reporter.report(new Persist(SafeEncoder.encode(key)));
  }

  public MessageFuture<Long> rpushx(String key, String... string) {
    return (MessageFuture<Long>) reporter.report(
        new RPushX(SafeEncoder.encode(key), SafeEncoder.encodeMany(string)));
  }

  public MessageFuture<String> echo(String string) {
    return transformResponse(
        reporter.report(new Echo(SafeEncoder.encode(string))), BuilderFactory.STRING);
  }

  public MessageFuture<Long> linsert(String key, LIST_POSITION where,
      String pivot, String value) {
    return (MessageFuture<Long>) reporter.report(
        new LInsert(SafeEncoder.encode(key), where, SafeEncoder.encode(pivot), SafeEncoder.encode(value)));
  }

  public String brpoplpush(String source, String destination, int timeout) {
    return null; // TODO
  }

  public Boolean setbit(String key, long offset, boolean value) {
    return null; // TODO
  }

  public Boolean setbit(String key, long offset, String value) {
    return null; // TODO
  }

  public MessageFuture<Boolean> getbit(String key, long offset) {
    return (MessageFuture<Boolean>) reporter.report(new GetBit(SafeEncoder.encode(key), offset));
  }

  public Long setrange(String key, long offset, String value) {
    return null; // TODO
  }

  public MessageFuture<String> getrange(String key, long startOffset, long endOffset) {
    return (MessageFuture<String>) reporter.report(new GetRange(key, startOffset, endOffset));
  }

  public MessageFuture<Long> bitpos(String key, boolean value) {
    return (MessageFuture<Long>) reporter.report(
        new BitPos(SafeEncoder.encode(key), value, new BitPosParams()));
  }

  public MessageFuture<Long> bitpos(String key, boolean value, redis.clients.jedis.BitPosParams params) {
    return (MessageFuture<Long>) reporter.report(
        new BitPos(SafeEncoder.encode(key), value, params));
  }

  public List<String> configGet(String pattern) {
    return null; // TODO
  }

  public String configSet(String parameter, String value) {
    return null; // TODO
  }

  public Object eval(String script, int keyCount, String... params) {
    return null; // TODO
  }

  public void subscribe(JedisPubSub jedisPubSub, String... channels) {
    // TODO
  }

  public Long publish(String channel, String message) {
    return null; // TODO
  }

  public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
    // TODO
  }

  public Object eval(String script, List<String> keys,
      List<String> args) {
    return null; // TODO
  }

  public Object eval(String script) {
    return null; // TODO
  }

  public Object evalsha(String script) {
    return null; // TODO
  }

  public Object evalsha(String sha1, List<String> keys,
      List<String> args) {
    return null; // TODO
  }

  public Object evalsha(String sha1, int keyCount, String... params) {
    return null; // TODO
  }

  public Boolean scriptExists(String sha1) {
    return null; // TODO
  }

  public List<Boolean> scriptExists(String... sha1) {
    return null; // TODO
  }

  public String scriptLoad(String script) {
    return null; // TODO
  }

  public List<Slowlog> slowlogGet() {
    return null; // TODO
  }

  public List<Slowlog> slowlogGet(long entries) {
    return null; // TODO
  }

  public Long objectRefcount(String string) {
    return null; // TODO
  }

  public String objectEncoding(String string) {
    return null; // TODO
  }

  public Long objectIdletime(String string) {
    return null; // TODO
  }

  public Long bitcount(String key) {
    return null; // TODO
  }

  public Long bitcount(String key, long start, long end) {
    return null; // TODO
  }

  public Long bitop(BitOP op, String destKey, String... srcKeys) {
    return null; // TODO
  }

  public List<Map<String, String>> sentinelMasters() {
    return null; // TODO
  }

  public List<String> sentinelGetMasterAddrByName(String masterName) {
    return null; // TODO
  }

  public Long sentinelReset(String pattern) {
    return null; // TODO
  }

  public List<Map<String, String>> sentinelSlaves(String masterName) {
    return null; // TODO
  }

  public String sentinelFailover(String masterName) {
    return null; // TODO
  }

  public String sentinelMonitor(String masterName, String ip, int port, int quorum) {
    return null; // TODO
  }

  public String sentinelRemove(String masterName) {
    return null; // TODO
  }

  public String sentinelSet(String masterName,
      Map<String, String> parameterMap) {
    return null; // TODO
  }

  public byte[] dump(String key) {
    return null; // TODO
  }

  public String restore(String key, int ttl, byte[] serializedValue) {
    return null; // TODO
  }

  @Deprecated
  public Long pexpire(String key, int milliseconds) {
    return null; // TODO
  }

  public Long pexpire(String key, long milliseconds) {
    return null; // TODO
  }

  public Long pexpireAt(String key, long millisecondsTimestamp) {
    return null; // TODO
  }

  public Long pttl(String key) {
    return null; // TODO
  }

  @Deprecated
  public String psetex(String key, int milliseconds, String value) {
    return null; // TODO
  }

  public String psetex(String key, long milliseconds, String value) {
    return null; // TODO
  }

  public MessageFuture<String> set(String key, String value, String nxxx) {
    return (MessageFuture<String>) reporter.report(
        new RedisCommand.Set(SafeEncoder.encode(key), SafeEncoder.encode(value), SafeEncoder.encode(nxxx), null, 0L));
  }

  public MessageFuture<String> set(String key, String value, String nxxx, String expx, int time) {
    return (MessageFuture<String>) reporter.report(
        new RedisCommand.Set(SafeEncoder.encode(key), SafeEncoder.encode(value), SafeEncoder.encode(nxxx), SafeEncoder.encode(expx), time));
  }

  public String clientKill(String client) {
    return null; // TODO
  }

  public String clientSetname(String name) {
    return null; // TODO
  }

  public String migrate(String host, int port, String key, int destinationDb, int timeout) {
    return null; // TODO
  }

  @Deprecated
  public ScanResult<String> scan(int cursor) {
    return null; // TODO
  }

  @Deprecated
  public ScanResult<String> scan(int cursor,
      ScanParams params) {
    return null; // TODO
  }

  @Deprecated
  public ScanResult<Entry<String, String>> hscan(
      String key, int cursor) {
    return null; // TODO
  }

  @Deprecated
  public ScanResult<Entry<String, String>> hscan(
      String key, int cursor, ScanParams params) {
    return null; // TODO
  }

  @Deprecated
  public ScanResult<String> sscan(String key, int cursor) {
    return null; // TODO
  }

  @Deprecated
  public ScanResult<String> sscan(String key, int cursor,
      ScanParams params) {
    return null; // TODO
  }

  @Deprecated
  public ScanResult<Tuple> zscan(String key, int cursor) {
    return null; // TODO
  }

  @Deprecated
  public ScanResult<Tuple> zscan(String key, int cursor,
      ScanParams params) {
    return null; // TODO
  }

  public ScanResult<String> scan(String cursor) {
    return null; // TODO
  }

  public ScanResult<String> scan(String cursor,
      ScanParams params) {
    return null; // TODO
  }

  public ScanResult<Entry<String, String>> hscan(
      String key, String cursor) {
    return null; // TODO
  }

  public ScanResult<Entry<String, String>> hscan(
      String key, String cursor, ScanParams params) {
    return null; // TODO
  }

  public ScanResult<String> sscan(String key, String cursor) {
    return null; // TODO
  }

  public ScanResult<String> sscan(String key, String cursor,
      ScanParams params) {
    return null; // TODO
  }

  public ScanResult<Tuple> zscan(String key,
      String cursor) {
    return null; // TODO
  }

  public ScanResult<Tuple> zscan(String key,
      String cursor, ScanParams params) {
    return null; // TODO
  }

  public String clusterNodes() {
    return null; // TODO
  }

  public String readonly() {
    return null; // TODO
  }

  public String clusterMeet(String ip, int port) {
    return null; // TODO
  }

  public String clusterReset(Reset resetType) {
    return null; // TODO
  }

  public String clusterAddSlots(int... slots) {
    return null; // TODO
  }

  public String clusterDelSlots(int... slots) {
    return null; // TODO
  }

  public String clusterInfo() {
    return null; // TODO
  }

  public List<String> clusterGetKeysInSlot(int slot, int count) {
    return null; // TODO
  }

  public String clusterSetSlotNode(int slot, String nodeId) {
    return null; // TODO
  }

  public String clusterSetSlotMigrating(int slot, String nodeId) {
    return null; // TODO
  }

  public String clusterSetSlotImporting(int slot, String nodeId) {
    return null; // TODO
  }

  public String clusterSetSlotStable(int slot) {
    return null; // TODO
  }

  public String clusterForget(String nodeId) {
    return null; // TODO
  }

  public String clusterFlushSlots() {
    return null; // TODO
  }

  public Long clusterKeySlot(String key) {
    return null; // TODO
  }

  public Long clusterCountKeysInSlot(int slot) {
    return null; // TODO
  }

  public String clusterSaveConfig() {
    return null; // TODO
  }

  public String clusterReplicate(String nodeId) {
    return null; // TODO
  }

  public List<String> clusterSlaves(String nodeId) {
    return null; // TODO
  }

  public String clusterFailover() {
    return null; // TODO
  }

  public List<Object> clusterSlots() {
    return null; // TODO
  }

  public String asking() {
    return null; // TODO
  }

  public List<String> pubsubChannels(String pattern) {
    return null; // TODO
  }

  public Long pubsubNumPat() {
    return null; // TODO
  }

  public Map<String, String> pubsubNumSub(String... channels) {
    return null; // TODO
  }

  public void close() {
    // TODO
  }

  public void setDataSource(Pool<Jedis> jedisPool) {
    // TODO
  }

  public Long pfadd(String key, String... elements) {
    return null; // TODO
  }

  public long pfcount(String key) {
    return 0L; // TODO
  }

  public long pfcount(String... keys) {
    return 0L; // TODO
  }

  public String pfmerge(String destkey, String... sourcekeys) {
    return null; // TODO
  }

  public MessageFuture<List<String>> blpop(int timeout, String key) {
    return (MessageFuture<List<String>>) reporter.report(
        new Blpop(timeout, SafeEncoder.encode(key)));
  }

  public MessageFuture<List<String>> brpop(int timeout, String key) {
    return (MessageFuture<List<String>>) reporter.report(
        new Brpop(timeout, SafeEncoder.encode(key)));
  }

  public Long geoadd(String key, double longitude, double latitude, String member) {
    return null; // TODO
  }

  public Long geoadd(String key,
      Map<String, GeoCoordinate> memberCoordinateMap) {
    return null; // TODO
  }

  public Double geodist(String key, String member1, String member2) {
    return null; // TODO
  }

  public Double geodist(String key, String member1, String member2, GeoUnit unit) {
    return null; // TODO
  }

  public List<String> geohash(String key, String... members) {
    return null; // TODO
  }

  public List<GeoCoordinate> geopos(String key,
      String... members) {
    return null; // TODO
  }

  public List<GeoRadiusResponse> georadius(String key, double longitude,
      double latitude, double radius, GeoUnit unit) {
    return null; // TODO
  }

  public List<GeoRadiusResponse> georadius(String key, double longitude,
      double latitude, double radius, GeoUnit unit,
      GeoRadiusParam param) {
    return null; // TODO
  }

  public List<GeoRadiusResponse> georadiusByMember(String key,
      String member, double radius, GeoUnit unit) {
    return null; // TODO
  }

  public List<GeoRadiusResponse> georadiusByMember(String key,
      String member, double radius, GeoUnit unit,
      GeoRadiusParam param) {
    return null; // TODO
  }

  public List<Long> bitfield(String key, String... arguments) {
    return null; // TODO
  }

  public String ping() {
    return null; // TODO
  }

  public MessageFuture<String> set(byte[] key, byte[] value) {
    return (MessageFuture<String>) reporter.report(new RedisCommand.Set(key, value, null, null, 0L));
  }

  public MessageFuture<String> set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) {
    return (MessageFuture<String>) reporter.report(new RedisCommand.Set(key, value, nxxx, expx, time));
  }

  public MessageFuture<byte[]> get(byte[] key) {
    return (MessageFuture<byte[]>) reporter.report(new Get(key));
  }

  public String quit() {
    return null; // TODO
  }

  public MessageFuture<Long> exists(byte[]... keys) {
    return (MessageFuture<Long>) reporter.report(new Exists(keys));
  }

  public MessageFuture<Boolean> exists(byte[] key) {
    return transformResponse(reporter.report(new Exists(key)), BuilderFactory.BOOLEAN);
  }

  public MessageFuture<Long> del(byte[]... keys) {
    return (MessageFuture<Long>) reporter.report(new Del(keys));
  }

  public MessageFuture<Long> del(byte[] key) {
    return (MessageFuture<Long>) reporter.report(new Del(key));
  }

  public String type(byte[] key) {
    return null; // TODO
  }

  public String flushDB() {
    return null; // TODO
  }

  public Set<byte[]> keys(byte[] pattern) {
    return null; // TODO
  }

  public byte[] randomBinaryKey() {
    return null; // TODO
  }

  public MessageFuture<String> rename(byte[] oldkey, byte[] newkey) {
    return (MessageFuture<String>) reporter.report(new Rename(oldkey, newkey));
  }

  public MessageFuture<Long> renamenx(byte[] oldkey, byte[] newkey) {
    return (MessageFuture<Long>) reporter.report(new RenameNX(oldkey, newkey));
  }

  public Long dbSize() {
    return null; // TODO
  }

  public MessageFuture<Long> expire(byte[] key, int seconds) {
    return (MessageFuture<Long>) reporter.report(new Expire(key, seconds));
  }

  public MessageFuture<Long> expireAt(byte[] key, long unixTime) {
    return (MessageFuture<Long>) reporter.report(new ExpireAt(key, unixTime));
  }

  public Long ttl(byte[] key) {
    return null; // TODO
  }

  public String select(int index) {
    return null; // TODO
  }

  public MessageFuture<Long> move(byte[] key, int dbIndex) {
    return (MessageFuture<Long>) reporter.report(new Move(key, dbIndex));
  }

  public String flushAll() {
    return null; // TODO
  }

  public MessageFuture<byte[]> getSet(byte[] key, byte[] value) {
    return (MessageFuture<byte[]>) reporter.report(new GetSet(key, value));
  }

  public MessageFuture<List<byte[]>> mget(byte[]... keys) {
    return (MessageFuture<List<byte[]>>) reporter.report(new MGet(keys));
  }

  public Long setnx(byte[] key, byte[] value) {
    return null; // TODO
  }

  public String setex(byte[] key, int seconds, byte[] value) {
    return null; // TODO
  }

  public MessageFuture<String> mset(byte[]... keysvalues) {
    return (MessageFuture<String>) reporter.report(new MSet(keysvalues));
  }

  public MessageFuture<Long> msetnx(byte[]... keysvalues) {
    return (MessageFuture<Long>) reporter.report(new MSetNX(keysvalues));
  }

  public MessageFuture<Long> decrBy(byte[] key, long integer) {
    return (MessageFuture<Long>) reporter.report(new DecrBy(key, integer));
  }

  public MessageFuture<Long> decr(byte[] key) {
    return (MessageFuture<Long>) reporter.report(new Decr(key));
  }

  public MessageFuture<Long> incrBy(byte[] key, long integer) {
    return (MessageFuture<Long>) reporter.report(new IncrBy(key, integer));
  }

  public Double incrByFloat(byte[] key, double integer) {
    return null; // TODO
  }

  public MessageFuture<Long> incr(byte[] key) {
    return (MessageFuture<Long>) reporter.report(new Incr(key));
  }

  public MessageFuture<Long> append(byte[] key, byte[] value) {
    return (MessageFuture<Long>) reporter.report(new Append(key, value));
  }

  public byte[] substr(byte[] key, int start, int end) {
    return null; // TODO
  }

  public MessageFuture<Long> hset(byte[] key, byte[] field, byte[] value) {
    return (MessageFuture<Long>) reporter.report(new HSet(key, field, value));
  }

  public MessageFuture<byte[]> hget(byte[] key, byte[] field) {
    return (MessageFuture<byte[]>) reporter.report(new HGet(key, field));
  }

  public MessageFuture<Long> hsetnx(byte[] key, byte[] field, byte[] value) {
    return (MessageFuture<Long>) reporter.report(new HSetNX(key, field, value));
  }

  public MessageFuture<String> hmset(byte[] key, Map<byte[], byte[]> hash) {
    return (MessageFuture<String>) reporter.report(new HMSet(key, hash));
  }

  public MessageFuture<List<byte[]>> hmget(byte[] key, byte[]... fields) {
    return (MessageFuture<List<byte[]>>) reporter.report(new HMGet(key, fields));
  }

  public MessageFuture<Long> hincrBy(byte[] key, byte[] field, long value) {
    return (MessageFuture<Long>) reporter.report(new HIncrBy(key, field, value));
  }

  public Double hincrByFloat(byte[] key, byte[] field, double value) {
    return null; // TODO
  }

  public MessageFuture<Boolean> hexists(byte[] key, byte[] field) {
    return (MessageFuture<Boolean>) reporter.report(new HExists(key, field));
  }

  public MessageFuture<Long> hdel(byte[] key, byte[]... fields) {
    return (MessageFuture<Long>) reporter.report(new HDel(key, fields));
  }

  public MessageFuture<Long> hlen(byte[] key) {
    return (MessageFuture<Long>) reporter.report(new HLen(key));
  }

  public MessageFuture<Set<byte[]>> hkeys(byte[] key) {
    return (MessageFuture<Set<byte[]>>) reporter.report(new HKeys(key));
  }

  public MessageFuture<List<byte[]>> hvals(byte[] key) {
    return (MessageFuture<List<byte[]>>) reporter.report(new HVals(key));
  }

  public MessageFuture<Map<byte[], byte[]>> hgetAll(byte[] key) {
    return (MessageFuture<Map<byte[], byte[]>>) reporter.report(new HGetAll(key));
  }

  public MessageFuture<Long> rpush(byte[] key, byte[]... strings) {
    return (MessageFuture<Long>) reporter.report(new RPush(key, strings));
  }

  public MessageFuture<Long> lpush(byte[] key, byte[]... strings) {
    return (MessageFuture<Long>) reporter.report(new LPush(key, strings));
  }

  public MessageFuture<Long> llen(byte[] key) {
    return (MessageFuture<Long>) reporter.report(new LLen(key));
  }

  public MessageFuture<List<byte[]>> lrange(byte[] key, long start, long end) {
    return (MessageFuture<List<byte[]>>) reporter.report(new LRange(key, start, end));
  }

  public MessageFuture<String> ltrim(byte[] key, long start, long end) {
    return (MessageFuture<String>) reporter.report(new LTrim(key, start, end));
  }

  public MessageFuture<byte[]> lindex(byte[] key, long index) {
    return (MessageFuture<byte[]>) reporter.report(new LIndex(key, index));
  }

  public MessageFuture<String> lset(byte[] key, long index, byte[] value) {
    return (MessageFuture<String>) reporter.report(new LSet(key, index, value));
  }

  public MessageFuture<Long> lrem(byte[] key, long count, byte[] value) {
    return (MessageFuture<Long>) reporter.report(new LRem(key, count, value));
  }

  public MessageFuture<byte[]> lpop(byte[] key) {
    return (MessageFuture<byte[]>) reporter.report(new LPop(key));
  }

  public MessageFuture<byte[]> rpop(byte[] key) {
    return (MessageFuture<byte[]>) reporter.report(new RPop(key));
  }

  public MessageFuture<byte[]> rpoplpush(byte[] srckey, byte[] dstkey) {
    return (MessageFuture<byte[]>) reporter.report(new RPopLpush(srckey, dstkey));
  }

  public MessageFuture<Long> sadd(byte[] key, byte[]... members) {
    return (MessageFuture<Long>) reporter.report(new SAdd(key, members));
  }

  public MessageFuture<Set<byte[]>> smembers(byte[] key) {
    return (MessageFuture<Set<byte[]>>) reporter.report(new SMembers(key));
  }

  public Long srem(byte[] key, byte[]... member) {
    return null; // TODO
  }

  public byte[] spop(byte[] key) {
    return null; // TODO
  }

  public Set<byte[]> spop(byte[] key, long count) {
    return null; // TODO
  }

  public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
    return null; // TODO
  }

  public Long scard(byte[] key) {
    return null; // TODO
  }

  public Boolean sismember(byte[] key, byte[] member) {
    return null; // TODO
  }

  public Set<byte[]> sinter(byte[]... keys) {
    return null; // TODO
  }

  public Long sinterstore(byte[] dstkey, byte[]... keys) {
    return null; // TODO
  }

  public Set<byte[]> sunion(byte[]... keys) {
    return null; // TODO
  }

  public Long sunionstore(byte[] dstkey, byte[]... keys) {
    return null; // TODO
  }

  public Set<byte[]> sdiff(byte[]... keys) {
    return null; // TODO
  }

  public Long sdiffstore(byte[] dstkey, byte[]... keys) {
    return null; // TODO
  }

  public byte[] srandmember(byte[] key) {
    return null; // TODO
  }

  public List<byte[]> srandmember(byte[] key, int count) {
    return null; // TODO
  }

  public Long zadd(byte[] key, double score, byte[] member) {
    return null; // TODO
  }

  public Long zadd(byte[] key, double score, byte[] member,
      ZAddParams params) {
    return null; // TODO
  }

  public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
    return null; // TODO
  }

  public Long zadd(byte[] key, Map<byte[], Double> scoreMembers,
      ZAddParams params) {
    return null; // TODO
  }

  public Set<byte[]> zrange(byte[] key, long start, long end) {
    return null; // TODO
  }

  public Long zrem(byte[] key, byte[]... members) {
    return null; // TODO
  }

  public Double zincrby(byte[] key, double score, byte[] member) {
    return null; // TODO
  }

  public Double zincrby(byte[] key, double score, byte[] member,
      ZIncrByParams params) {
    return null; // TODO
  }

  public Long zrank(byte[] key, byte[] member) {
    return null; // TODO
  }

  public Long zrevrank(byte[] key, byte[] member) {
    return null; // TODO
  }

  public Set<byte[]> zrevrange(byte[] key, long start, long end) {
    return null; // TODO
  }

  public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
    return null; // TODO
  }

  public Set<Tuple> zrevrangeWithScores(byte[] key, long start,
      long end) {
    return null; // TODO
  }

  public Long zcard(byte[] key) {
    return null; // TODO
  }

  public Double zscore(byte[] key, byte[] member) {
    return null; // TODO
  }

  public Transaction multi() {
    return null; // TODO
  }

  @Deprecated
  public List<Object> multi(TransactionBlock jedisTransaction) {
    return null; // TODO
  }

  public void connect() {
    // TODO
  }

  public void disconnect() {
    // TODO
  }

  public void resetState() {
    // TODO
  }

  public String watch(byte[]... keys) {
    return null; // TODO
  }

  public String unwatch() {
    return null; // TODO
  }

  public List<byte[]> sort(byte[] key) {
    return null; // TODO
  }

  public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
    return null; // TODO
  }

  public MessageFuture<List<byte[]>> blpop(int timeout, byte[]... keys) {
    return transformResponse(
        reporter.report(new Blpop(timeout, keys)), BuilderFactory.BYTE_ARRAY_LIST);
  }

  public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
    return null; // TODO
  }

  public Long sort(byte[] key, byte[] dstkey) {
    return null; // TODO
  }

  public MessageFuture<List<byte[]>> brpop(int timeout, byte[]... keys) {
    return transformResponse(
        reporter.report(new Brpop(timeout, keys)), BuilderFactory.BYTE_ARRAY_LIST);
  }

  @Deprecated
  public MessageFuture<List<byte[]>> blpop(byte[] arg) {
    return (MessageFuture<List<byte[]>>) reporter.report(new Blpop(0, arg));
  }

  @Deprecated
  public MessageFuture<List<byte[]>> brpop(byte[] arg) {
    return (MessageFuture<List<byte[]>>) reporter.report(new Brpop(0, arg));
  }

  public MessageFuture<List<byte[]>> blpop(byte[]... args) {
    return (MessageFuture<List<byte[]>>) reporter.report(new Blpop(0, args));
  }

  public MessageFuture<List<byte[]>> brpop(byte[]... args) {
    return (MessageFuture<List<byte[]>>) reporter.report(new Brpop(0, args));
  }

  public String auth(String password) {
    return null; // TODO
  }

  @Deprecated
  public List<Object> pipelined(PipelineBlock jedisPipeline) {
    return null; // TODO
  }

  public Pipeline pipelined() {
    return null; // TODO
  }

  public Long zcount(byte[] key, double min, double max) {
    return null; // TODO
  }

  public Long zcount(byte[] key, byte[] min, byte[] max) {
    return null; // TODO
  }

  public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
    return null; // TODO
  }

  public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
    return null; // TODO
  }

  public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset,
      int count) {
    return null; // TODO
  }

  public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset,
      int count) {
    return null; // TODO
  }

  public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min,
      double max) {
    return null; // TODO
  }

  public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min,
      byte[] max) {
    return null; // TODO
  }

  public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min,
      double max, int offset, int count) {
    return null; // TODO
  }

  public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min,
      byte[] max, int offset, int count) {
    return null; // TODO
  }

  public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
    return null; // TODO
  }

  public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
    return null; // TODO
  }

  public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset,
      int count) {
    return null; // TODO
  }

  public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset,
      int count) {
    return null; // TODO
  }

  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max,
      double min) {
    return null; // TODO
  }

  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max,
      double min, int offset, int count) {
    return null; // TODO
  }

  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max,
      byte[] min) {
    return null; // TODO
  }

  public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max,
      byte[] min, int offset, int count) {
    return null; // TODO
  }

  public Long zremrangeByRank(byte[] key, long start, long end) {
    return null; // TODO
  }

  public Long zremrangeByScore(byte[] key, double start, double end) {
    return null; // TODO
  }

  public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
    return null; // TODO
  }

  public Long zunionstore(byte[] dstkey, byte[]... sets) {
    return null; // TODO
  }

  public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
    return null; // TODO
  }

  public Long zinterstore(byte[] dstkey, byte[]... sets) {
    return null; // TODO
  }

  public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
    return null; // TODO
  }

  public Long zlexcount(byte[] key, byte[] min, byte[] max) {
    return null; // TODO
  }

  public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
    return null; // TODO
  }

  public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
    return null; // TODO
  }

  public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
    return null; // TODO
  }

  public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset,
      int count) {
    return null; // TODO
  }

  public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
    return null; // TODO
  }

  public String save() {
    return null; // TODO
  }

  public String bgsave() {
    return null; // TODO
  }

  public String bgrewriteaof() {
    return null; // TODO
  }

  public Long lastsave() {
    return null; // TODO
  }

  public String shutdown() {
    return null; // TODO
  }

  public String info() {
    return null; // TODO
  }

  public String info(String section) {
    return null; // TODO
  }

  public void monitor(JedisMonitor jedisMonitor) {
    // TODO
  }

  public String slaveof(String host, int port) {
    return null; // TODO
  }

  public String slaveofNoOne() {
    return null; // TODO
  }

  public List<byte[]> configGet(byte[] pattern) {
    return null; // TODO
  }

  public String configResetStat() {
    return null; // TODO
  }

  public byte[] configSet(byte[] parameter, byte[] value) {
    return null; // TODO
  }

  public boolean isConnected() {
    return false; // TODO
  }

  public Long strlen(byte[] key) {
    return null; // TODO
  }

  public void sync() {
    // TODO
  }

  public MessageFuture<Long> lpushx(byte[] key, byte[]... string) {
    return (MessageFuture<Long>) reporter.report(new LPushX(key, string));
  }

  public MessageFuture<Long> persist(byte[] key) {
    return (MessageFuture<Long>) reporter.report(new Persist(key));
  }

  public MessageFuture<Long> rpushx(byte[] key, byte[]... string) {
    return (MessageFuture<Long>) reporter.report(new RPushX(key, string));
  }

  public MessageFuture<byte[]> echo(byte[] string) {
    return (MessageFuture<byte[]>) reporter.report(new Echo(string));
  }

  public MessageFuture<Long> linsert(byte[] key, LIST_POSITION where, byte[] pivot,
      byte[] value) {
    return (MessageFuture<Long>) reporter.report(new LInsert(key, where, pivot, value));
  }

  public String debug(DebugParams params) {
    return null; // TODO
  }

  /**
   * Client returned here is not thread-safe
   */
  public Client getClient() {
    Jedis jedis = jedisPool.getResource();
    try {
      return jedis.getClient();
    } finally {
      jedis.close();
    }
  }

  public MessageFuture<byte[]> brpoplpush(byte[] source, byte[] destination, int timeout) {
    return (MessageFuture<byte[]>) reporter.report(new BrpopLpush(timeout, source, destination));
  }

  public Boolean setbit(byte[] key, long offset, boolean value) {
    return null; // TODO
  }

  public Boolean setbit(byte[] key, long offset, byte[] value) {
    return null; // TODO
  }

  public MessageFuture<Boolean> getbit(byte[] key, long offset) {
    return (MessageFuture<Boolean>) reporter.report(new GetBit(key, offset));
  }

  public MessageFuture<Long> bitpos(byte[] key, boolean value) {
    return (MessageFuture<Long>) reporter.report(new BitPos(key, value, new BitPosParams()));
  }

  public MessageFuture<Long> bitpos(byte[] key, boolean value, redis.clients.jedis.BitPosParams params) {
    return (MessageFuture<Long>) reporter.report(new BitPos(key, value, params));
  }

  public Long setrange(byte[] key, long offset, byte[] value) {
    return null; // TODO
  }

  public MessageFuture<byte[]> getrange(byte[] key, long startOffset, long endOffset) {
    return transformResponse(reporter.report(
        new GetRange(SafeEncoder.encode(key), startOffset, endOffset)), BuilderFactory.BYTE_ARRAY);
  }

  public Long publish(byte[] channel, byte[] message) {
    return null; // TODO
  }

  public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
    // TODO
  }

  public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
    // TODO
  }

  public Long getDB() {
    return null; // TODO
  }

  public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
    return null; // TODO
  }

  public Object eval(byte[] script, byte[] keyCount, byte[]... params) {
    return null; // TODO
  }

  public Object eval(byte[] script, int keyCount, byte[]... params) {
    return null; // TODO
  }

  public Object eval(byte[] script) {
    return null; // TODO
  }

  public Object evalsha(byte[] sha1) {
    return null; // TODO
  }

  public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
    return null; // TODO
  }

  public Object evalsha(byte[] sha1, int keyCount, byte[]... params) {
    return null; // TODO
  }

  public String scriptFlush() {
    return null; // TODO
  }

  public Long scriptExists(byte[] sha1) {
    return null; // TODO
  }

  public List<Long> scriptExists(byte[]... sha1) {
    return null; // TODO
  }

  public byte[] scriptLoad(byte[] script) {
    return null; // TODO
  }

  public String scriptKill() {
    return null; // TODO
  }

  public String slowlogReset() {
    return null; // TODO
  }

  public Long slowlogLen() {
    return null; // TODO
  }

  public List<byte[]> slowlogGetBinary() {
    return null; // TODO
  }

  public List<byte[]> slowlogGetBinary(long entries) {
    return null; // TODO
  }

  public Long objectRefcount(byte[] key) {
    return null; // TODO
  }

  public byte[] objectEncoding(byte[] key) {
    return null; // TODO
  }

  public Long objectIdletime(byte[] key) {
    return null; // TODO
  }

  public Long bitcount(byte[] key) {
    return null; // TODO
  }

  public Long bitcount(byte[] key, long start, long end) {
    return null; // TODO
  }

  public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
    return null; // TODO
  }

  public byte[] dump(byte[] key) {
    return null; // TODO
  }

  public String restore(byte[] key, int ttl, byte[] serializedValue) {
    return null; // TODO
  }

  @Deprecated
  public Long pexpire(byte[] key, int milliseconds) {
    return null; // TODO
  }

  public Long pexpire(byte[] key, long milliseconds) {
    return null; // TODO
  }

  public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
    return null; // TODO
  }

  public Long pttl(byte[] key) {
    return null; // TODO
  }

  @Deprecated
  public String psetex(byte[] key, int milliseconds, byte[] value) {
    return null; // TODO
  }

  public String psetex(byte[] key, long milliseconds, byte[] value) {
    return null; // TODO
  }

  public MessageFuture<String> set(byte[] key, byte[] value, byte[] nxxx) {
    return (MessageFuture<String>) reporter.report(new RedisCommand.Set(key, value, nxxx, null, 0L));
  }

  public MessageFuture<String> set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, int time) {
    return (MessageFuture<String>) reporter.report(new RedisCommand.Set(key, value, nxxx, expx, time));
  }

  public String clientKill(byte[] client) {
    return null; // TODO
  }

  public String clientGetname() {
    return null; // TODO
  }

  public String clientList() {
    return null; // TODO
  }

  public String clientSetname(byte[] name) {
    return null; // TODO
  }

  public List<String> time() {
    return null; // TODO
  }

  public String migrate(byte[] host, int port, byte[] key, int destinationDb, int timeout) {
    return null; // TODO
  }

  public Long waitReplicas(int replicas, long timeout) {
    return null; // TODO
  }

  public Long pfadd(byte[] key, byte[]... elements) {
    return null; // TODO
  }

  public long pfcount(byte[] key) {
    return 0L; // TODO
  }

  public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
    return null; // TODO
  }

  public Long pfcount(byte[]... keys) {
    return null; // TODO
  }

  public ScanResult<byte[]> scan(byte[] cursor) {
    return null; // TODO
  }

  public ScanResult<byte[]> scan(byte[] cursor,
      ScanParams params) {
    return null; // TODO
  }

  public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key,
      byte[] cursor) {
    return null; // TODO
  }

  public ScanResult<Entry<byte[], byte[]>> hscan(byte[] key,
      byte[] cursor, ScanParams params) {
    return null; // TODO
  }

  public ScanResult<byte[]> sscan(byte[] key, byte[] cursor) {
    return null; // TODO
  }

  public ScanResult<byte[]> sscan(byte[] key, byte[] cursor,
      ScanParams params) {
    return null; // TODO
  }

  public ScanResult<Tuple> zscan(byte[] key, byte[] cursor) {
    return null; // TODO
  }

  public ScanResult<Tuple> zscan(byte[] key, byte[] cursor,
      ScanParams params) {
    return null; // TODO
  }

  public Long geoadd(byte[] key, double longitude, double latitude, byte[] member) {
    return null; // TODO
  }

  public Long geoadd(byte[] key,
      Map<byte[], GeoCoordinate> memberCoordinateMap) {
    return null; // TODO
  }

  public Double geodist(byte[] key, byte[] member1, byte[] member2) {
    return null; // TODO
  }

  public Double geodist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit) {
    return null; // TODO
  }

  public List<byte[]> geohash(byte[] key, byte[]... members) {
    return null; // TODO
  }

  public List<GeoCoordinate> geopos(byte[] key, byte[]... members) {
    return null; // TODO
  }

  public List<GeoRadiusResponse> georadius(byte[] key, double longitude,
      double latitude, double radius, GeoUnit unit) {
    return null; // TODO
  }

  public List<GeoRadiusResponse> georadius(byte[] key, double longitude,
      double latitude, double radius, GeoUnit unit,
      GeoRadiusParam param) {
    return null; // TODO
  }

  public List<GeoRadiusResponse> georadiusByMember(byte[] key,
      byte[] member, double radius, GeoUnit unit) {
    return null; // TODO
  }

  public List<GeoRadiusResponse> georadiusByMember(byte[] key,
      byte[] member, double radius, GeoUnit unit,
      GeoRadiusParam param) {
    return null; // TODO
  }

  public List<byte[]> bitfield(byte[] key, byte[]... arguments) {
    return null; // TODO
  }

  public JedisPool unwrapped() {
    return jedisPool;
  }
}
