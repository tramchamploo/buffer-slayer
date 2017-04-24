package io.github.tramchamploo.bufferslayer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import redis.clients.util.SafeEncoder;

/**
 * Factory for {@link Builder} that builds result
 */
final class BuilderFactory {

  @SuppressWarnings("unchecked")
  static final Builder<List<byte[]>> BYTE_ARRAY_LIST = new Builder<List<byte[]>>() {
    @Override
    List<byte[]> build(Object data) {
      if (null == data) {
        return Collections.emptyList();
      }
      List<String> l = (List<String>) data;
      final ArrayList<byte[]> result = new ArrayList<>(l.size());
      for (final String s: l) {
        if (s == null) {
          result.add(null);
        } else {
          result.add(SafeEncoder.encode(s));
        }
      }
      return result;
    }
  };

  static final Builder<String> STRING = new Builder<String>() {
    public String build(Object data) {
      return data == null ? null : SafeEncoder.encode((byte[]) data);
    }
  };

  static final Builder<Boolean> BOOLEAN = new Builder<Boolean>() {
    public Boolean build(Object data) {
      return ((Long) data) == 1;
    }
  };

  static final Builder<List<String>> STRING_LIST = new Builder<List<String>>() {
    @SuppressWarnings("unchecked")
    public List<String> build(Object data) {
      if (null == data) {
        return Collections.emptyList();
      }
      List<byte[]> l = (List<byte[]>) data;
      final ArrayList<String> result = new ArrayList<String>(l.size());
      for (final byte[] barray : l) {
        if (barray == null) {
          result.add(null);
        } else {
          result.add(SafeEncoder.encode(barray));
        }
      }
      return result;
    }
  };

  public static final Builder<byte[]> BYTE_ARRAY = new Builder<byte[]>() {
    public byte[] build(Object data) {
      return SafeEncoder.encode((String) data);
    }
  };

  public static final Builder<Map<String, String>> STRING_MAP = new Builder<Map<String, String>>() {
    @SuppressWarnings("unchecked")
    public Map<String, String> build(Object data) {
      final Map<byte[], byte[]> byteMap = (Map<byte[], byte[]>) data;
      final Map<String, String> hash = new HashMap<>(byteMap.size(), 1);
      Iterator<Entry<byte[], byte[]>> iterator = byteMap.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<byte[], byte[]> entry = iterator.next();
        hash.put(SafeEncoder.encode(entry.getKey()), SafeEncoder.encode(entry.getValue()));
      }

      return hash;
    }
  };

  public static final Builder<Set<String>> STRING_SET = new Builder<Set<String>>() {
    @SuppressWarnings("unchecked")
    public Set<String> build(Object data) {
      if (null == data) {
        return Collections.emptySet();
      }
      Set<byte[]> l = (Set<byte[]>) data;
      final Set<String> result = new HashSet<>(l.size());
      for (final byte[] barray : l) {
        if (barray == null) {
          result.add(null);
        } else {
          result.add(SafeEncoder.encode(barray));
        }
      }
      return result;
    }
  };
}
