package io.github.tramchamploo.bufferslayer;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.jdeferred.Deferred;
import org.jdeferred.impl.DeferredObject;

/**
 * Created by tramchamploo on 2017/4/12.
 * Maintain a mapping from messageId to Deferred
 */
class DeferredHolder {

  private static final ConcurrentHashMap<Long, Deferred> holder = new ConcurrentHashMap<>();

  static <D> Deferred<D, MessageDroppedException, Integer> newDeferred(Long id) {
    Deferred<D, MessageDroppedException, Integer> deferred = new DeferredObject<>();
    holder.put(id, deferred);
    return deferred;
  }

  static <T extends Message> void batchResolve(List<T> messages, List<?> resolves) {
    for (int i = 0, j = 0; i < messages.size() && j < resolves.size(); i++) {
      if (resolve(messages.get(i).id, resolves.get(i))) {
        j++;
      }
    }
  }

  @SuppressWarnings("unchecked")
  static boolean resolve(Long id, Object resolve) {
    Deferred deferred = holder.get(id);
    boolean success = !deferred.isRejected();
    if (success) {
      deferred.resolve(resolve);
    }
    holder.remove(id);
    return success;
  }

  static void reject(MessageDroppedException ex) {
    List<? extends Message> dropped = ex.dropped;
    for (Message message : dropped) {
      doReject(message, ex);
    }
  }

  @SuppressWarnings("unchecked")
  private static Deferred doReject(Message message, MessageDroppedException ex) {
    Deferred deferred = holder.get(message.id);
    deferred.reject(ex);
    holder.remove(message.id);
    return deferred;
  }

  static <T extends Message> void batchReject(List<T> messages, MessageDroppedException ex) {
    for (T message : messages) {
      doReject(message, ex);
    }
  }
}
