package io.github.tramchamploo.bufferslayer;

import com.google.common.base.Preconditions;
import io.github.tramchamploo.bufferslayer.chmv8.LongAdderV8;

/**
 * Record total messages in queue
 */
abstract class MessageCounter {

  static MessageCounter maxOf(int maxMessages) {
    Preconditions.checkArgument(maxMessages > 0, "maxMessages should be greater than 0.");
    int result = Math.min(Integer.MAX_VALUE, maxMessages);
    return new DefaultMessageCounter(result);
  }

  abstract boolean isMaximum();

  abstract void increment();

  abstract void add(long value);

  private static final class DefaultMessageCounter extends MessageCounter {
    private final long maxMessages;
    private final LongAdderV8 queuedMessages = new LongAdderV8();

    private DefaultMessageCounter(long maxMessages) {
      this.maxMessages = maxMessages;
    }

    boolean isMaximum() {
      return queuedMessages.sum() >= maxMessages;
    }

    void increment() {
      queuedMessages.increment();
    }

    void add(long value) {
      queuedMessages.add(value);
    }
  }
}

