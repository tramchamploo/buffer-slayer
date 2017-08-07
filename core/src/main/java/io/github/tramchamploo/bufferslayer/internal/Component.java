package io.github.tramchamploo.bufferslayer.internal;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;

/**
 * Adapted from zipkin
 */
public interface Component extends Closeable {

  CheckResult check();

  @Override
  void close() throws IOException;

  final class CheckResult {

    public static final CheckResult OK = new CheckResult(true, null);

    public static final CheckResult failed(Exception exception) {
      return new CheckResult(false,
          Preconditions.checkNotNull(exception, "exception must not be null"));
    }

    public final boolean ok;

    /**
     * Present when not ok
     */
    public final Exception exception;

    CheckResult(boolean ok, Exception exception) {
      this.ok = ok;
      this.exception = exception;
    }
  }
}
