package io.github.tramchamploo.bufferslayer;

import redis.clients.jedis.Pipeline;

/**
 * A redis command with multi keys
 */
abstract class MultiKeyRedisCommand extends RedisCommand {

    final byte[][] keys;

    MultiKeyRedisCommand(byte[][] keys) {
      super(keys[0]);
      this.keys = keys;
    }

    String keysString() {
      return RedisCommandUtil.toString(keys);
    }

  // Commands start

  final static class Blpop extends MultiKeyRedisCommand {

    final int timeout;

    Blpop(int timeout, byte[]... keys) {
      super(keys);
      this.timeout = timeout;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      if (timeout == 0) {
        pipeline.blpop(keys);
      } else {
        pipeline.blpop(timeout, keys);
      }
    }

    @Override
    public String toString() {
      return "Blpop(" + keysString() + ", " + timeout + ")";
    }
  }

  final static class Brpop extends MultiKeyRedisCommand {

    final int timeout;

    Brpop(int timeout, byte[]... keys) {
      super(keys);
      this.timeout = timeout;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      if (timeout == 0) {
        pipeline.brpop(keys);
      } else {
        pipeline.brpop(timeout, keys);
      }
    }

    @Override
    public String toString() {
      return "Brpop(" + keysString() + ", " + timeout + ")";
    }
  }

  final static class Del extends MultiKeyRedisCommand {

    Del(byte[]... keys) {
      super(keys);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.del(keys);
    }

    @Override
    public String toString() {
      return "Del(" + keysString() + ")";
    }
  }

  final static class Exists extends MultiKeyRedisCommand {

    Exists(byte[]... keys) {
      super(keys);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.exists(keys);
    }

    @Override
    public String toString() {
      return "Exists(" + keysString() + ")";
    }
  }

  final static class MGet extends MultiKeyRedisCommand {

    MGet(byte[]... keys) {
      super(keys);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.mget(keys);
    }

    @Override
    public String toString() {
      return "MGet(" + keysString() + ")";
    }
  }

  final static class MSet extends MultiKeyRedisCommand {

    MSet(byte[]... keysvalues) {
      super(keysvalues);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.mset(keys);
    }

    @Override
    public String toString() {
      return "MSet(" + keysString() + ")";
    }
  }

  final static class MSetNX extends MultiKeyRedisCommand {

    MSetNX(byte[]... keysvalues) {
      super(keysvalues);
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.msetnx(keys);
    }

    @Override
    public String toString() {
      return "MSetNX(" + keysString() + ")";
    }
  }

  final static class BrpopLpush extends MultiKeyRedisCommand {

    final int timeout;

    BrpopLpush(int timeout, byte[]... keys) {
      super(keys);
      this.timeout = timeout;
    }

    @Override
    protected void apply(Pipeline pipeline) {
      pipeline.brpoplpush(keys[0], keys[1], timeout);
    }

    @Override
    public String toString() {
      return "BrpopLpush(" + keysString() + ", " + timeout + ")";
    }
  }
}