package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.internal.SendingTask;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

final class Buffer<M extends Message> implements SizeBoundedQueue.Consumer<M> {

  private final int maxSize;
  private final boolean onlyAcceptSame;

  final List<SendingTask<M>> buffer = new LinkedList<>();
  boolean bufferFull;
  Message.MessageKey lastMessageKey;
  boolean ofTheSameKey = true;
  Buffer next;

  Buffer(int maxSize, boolean onlyAcceptSame) {
    this.maxSize = maxSize;
    this.onlyAcceptSame = onlyAcceptSame;
  }

  @Override
  public boolean accept(SendingTask<M> next) {
    if (bufferFull) {
      return false;
    }
    if (onlyAcceptSame) {
      MessageKey nextKey = next.message.asMessageKey();
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

  List<SendingTask<M>> drain() {
    if (buffer.isEmpty()) {
      return Collections.emptyList();
    }
    ArrayList<SendingTask<M>> result = new ArrayList<>(buffer);
    clear();
    return result;
  }

  void clear() {
    buffer.clear();
    bufferFull = false;
    lastMessageKey = null;
    ofTheSameKey = true;
  }
}
