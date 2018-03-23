package io.github.tramchamploo.bufferslayer.internal;

import static io.github.tramchamploo.bufferslayer.MessageDroppedException.dropped;

import com.google.common.base.Preconditions;
import io.github.tramchamploo.bufferslayer.Message;
import io.github.tramchamploo.bufferslayer.MessageDroppedException;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.LinkedList;
import java.util.List;

public final class Promises {

  /**
   * Set a successful result to all promises
   */
  public static <R> void allSuccess(List<R> result, List<MessagePromise<R>> promises) {
    Preconditions.checkArgument(result.size() == promises.size());
    for (int i = 0; i < result.size(); i++) {
      MessagePromise<R> promise = promises.get(i);
      R ret = result.get(i);
      promise.setSuccess(ret);
    }
  }

  /**
   * Set a failed result to all promises
   */
  public static void allFail(Throwable t,
      List<? extends MessagePromise<?>> promises, List<? extends Message> messages) {
    Preconditions.checkArgument(promises.size() == messages.size());
    for (MessagePromise<?> promise : promises) {
      promise.setFailure(MessageDroppedException.dropped(t, messages));
    }
  }

  /**
   * Set a failed result to all promises
   */
  public static void allFail(List<MessagePromise<?>> promises, Strategy overflowStrategy) {
    List<Message> messages = new LinkedList<>();
    for (MessagePromise<?> promise : promises) {
      messages.add(promise.message());
    }
    for (MessagePromise<?> promise : promises) {
      promise.setFailure(dropped(overflowStrategy, messages));
    }
  }

  private Promises() {}
}
