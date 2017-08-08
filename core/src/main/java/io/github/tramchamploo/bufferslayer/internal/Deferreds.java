package io.github.tramchamploo.bufferslayer.internal;

import com.google.common.base.Preconditions;
import io.github.tramchamploo.bufferslayer.Message;
import io.github.tramchamploo.bufferslayer.MessageDroppedException;
import java.util.List;
import org.jdeferred.Deferred;

@SuppressWarnings("unchecked")
public final class Deferreds {

  public static <R> void resolveAll(List<R> result, List<Deferred> deferreds) {
    Preconditions.checkArgument(result.size() == deferreds.size());
    for (int i = 0; i < result.size(); i++) {
      Deferred deferred = deferreds.get(i);
      R ret = result.get(i);
      deferred.resolve(ret);
    }
  }

  public static <M extends Message> void rejectAll(Throwable t, List<Deferred> deferreds, List<M> messages) {
    for (Deferred deferred : deferreds) {
      deferred.reject(MessageDroppedException.dropped(t, messages));
    }
  }

  private Deferreds() {}
}
