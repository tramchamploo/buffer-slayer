package io.github.tramchamploo.bufferslayer;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import redis.clients.util.SafeEncoder;

class RedisCommandUtil {

  static String toString(byte[][] bArray) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < bArray.length; i++) {
      if (i > 0) {
        builder.append(", ");
      }
      builder.append(SafeEncoder.encode(bArray[i]));
    }
    return builder.toString();
  }

  static String toString(Map<byte[], byte[]> bMap) {
    StringBuilder builder = new StringBuilder();
    Iterator<Entry<byte[], byte[]>> iterator = bMap.entrySet().iterator();
    int count = 0;

    while (iterator.hasNext()) {
      Entry<byte[], byte[]> entry = iterator.next();
      if (count > 0) {
        builder.append(", ");
      }

      builder.append(SafeEncoder.encode(entry.getKey()))
        .append("=")
        .append(SafeEncoder.encode(entry.getValue()));

      count++;
    }
    return builder.toString();
  }
}
