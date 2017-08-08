package io.github.tramchamploo.bufferslayer.internal;

public final class Properties {

  public static int propertyOr(String key, int fallback) {
    return System.getProperty(key) != null ? Integer.parseInt(System.getProperty(key)) : fallback;
  }

  private Properties() {}
}
