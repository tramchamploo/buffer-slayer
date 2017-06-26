package io.github.tramchamploo.bufferslayer.internal;

/**
 * Created by tramchamploo on 2017/6/26.
 */
public final class Util {

  public static int propertyOr(String key, int fallback) {
    return System.getProperty(key) != null ? Integer.parseInt(System.getProperty(key)) : fallback;
  }

  private Util() {}
}
