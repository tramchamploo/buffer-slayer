package io.github.tramchamploo.bufferslayer;

import com.google.common.base.Strings;
import java.util.concurrent.ThreadLocalRandom;

public class TestUtil {

  static String randomString() {
    return String.valueOf(ThreadLocalRandom.current().nextLong());
  }

  static String envOr(String key, String fallback) {
    return Strings.isNullOrEmpty(System.getenv(key)) ? fallback : System.getenv(key);
  }

  static String propertyOr(String key, String fallback) {
    return System.getProperty(key, fallback);
  }
}
