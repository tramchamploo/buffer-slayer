package io.github.tramchamploo.bufferslayer;

import static java.util.Collections.singletonList;

import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.List;

/**
 * Created by tramchamploo on 2017/4/12.
 * Exception raised when deferred is rejected.
 */
public class MessageDroppedException extends RuntimeException {

  final List<? extends Message> dropped;
  final boolean overflow;

  private MessageDroppedException(Strategy overflowStrategy, List<Message> dropped) {
    super("Dropped " + dropped.size() + " messages due to OverflowStrategy." + overflowStrategy);
    this.dropped = dropped;
    this.overflow = true;
  }

  private MessageDroppedException(Throwable cause, List<? extends Message> dropped) {
    super(String.format("Dropped %d messages due to %s(%s)",
        dropped.size(),
        cause.getClass().getSimpleName(),
        cause.getMessage() == null ? "" : cause.getMessage()),
        cause);
    this.dropped = dropped;
    this.overflow = false;
  }

  public static MessageDroppedException dropped(Strategy overflowStrategy, Message dropped) {
    return new MessageDroppedException(overflowStrategy, singletonList(dropped));
  }

  public static MessageDroppedException dropped(Strategy overflowStrategy, List<Message> dropped) {
    return new MessageDroppedException(overflowStrategy, dropped);
  }

  public static MessageDroppedException dropped(Throwable cause, List<? extends Message> dropped) {
    return new MessageDroppedException(cause, dropped);
  }
}
