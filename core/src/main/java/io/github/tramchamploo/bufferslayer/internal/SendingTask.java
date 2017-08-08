package io.github.tramchamploo.bufferslayer.internal;

import io.github.tramchamploo.bufferslayer.Message;
import java.util.ArrayList;
import java.util.List;
import org.jdeferred.Deferred;

/**
 * Task holding message and its deferred object
 */
public final class SendingTask<T extends Message> {

  public final T message;
  public final Deferred<Object, Object, Object> deferred;

  public SendingTask(T message, Deferred<Object, Object, Object> deferred) {
    this.message = message;
    this.deferred = deferred;
  }

  public static <M extends Message> Object[] unzipGeneric(final List<SendingTask<M>> tasks) {
    Object[] result = new Object[2];
    List<M> messages = new ArrayList<>();
    List<Deferred> deferreds = new ArrayList<>();
    for (SendingTask<M> task: tasks) {
      messages.add(task.message);
      deferreds.add(task.deferred);
    }
    result[0] = messages;
    result[1] = deferreds;
    return result;
  }

  public static Object[] unzip(final List<SendingTask> tasks) {
    Object[] result = new Object[2];
    List<Message> messages = new ArrayList<>();
    List<Deferred> deferreds = new ArrayList<>();
    for (SendingTask task: tasks) {
      messages.add(task.message);
      deferreds.add(task.deferred);
    }
    result[0] = messages;
    result[1] = deferreds;
    return result;
  }
}
