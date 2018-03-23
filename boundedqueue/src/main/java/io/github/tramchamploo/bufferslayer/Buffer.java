package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * A buffer that gathers messages which will be sent in the same batch later
 */
final class Buffer implements SizeBoundedQueue.Consumer {

  private final int maxSize;
  private final boolean onlyAcceptSame;

  final List<Object> buffer = new LinkedList<>();
  boolean bufferFull;
  Message.MessageKey lastMessageKey;
  boolean ofTheSameKey = true;
  Buffer next;

  Buffer(int maxSize, boolean onlyAcceptSame) {
    this.maxSize = maxSize;
    this.onlyAcceptSame = onlyAcceptSame;
  }

  @Override
  public boolean accept(MessagePromise<?> next) {
    if (bufferFull) {
      return false;
    }
    if (onlyAcceptSame) {
      MessageKey nextKey = next.message().asMessageKey();
      if (lastMessageKey == null) {
        lastMessageKey = nextKey;
      } else if (!lastMessageKey.equals(nextKey)) {
        ofTheSameKey = false;
        return false;
      }
    }
    buffer.add(next);
    if (buffer.size() == maxSize) bufferFull = true;
    return true;
  }

  @SuppressWarnings("unchecked")
  <T> T drain() {
    if (buffer.isEmpty()) {
      return (T) Collections.emptyList();
    }
    Object result = new ArrayList<>(buffer);
    clear();
    return (T) result;
  }

  void clear() {
    buffer.clear();
    bufferFull = false;
    lastMessageKey = null;
    ofTheSameKey = true;
  }
}
