package io.bufferslayer;

import io.bufferslayer.Message.MessageKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

final class Buffer implements SizeBoundedQueue.Consumer {

  private final int maxSize;
  private final boolean onlyAcceptSame;

  final List<Message> buffer = new LinkedList<>();
  boolean bufferFull;
  Message.MessageKey lastMessageKey;
  boolean ofTheSameKey = true;
  Buffer next;

  Buffer(int maxSize, boolean onlyAcceptSame) {
    this.maxSize = maxSize;
    this.onlyAcceptSame = onlyAcceptSame;
  }

  @Override
  public boolean accept(Message next) {
    if (bufferFull) {
      return false;
    }
    if (onlyAcceptSame) {
      MessageKey nextKey = next.asMessageKey();
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

  List<Message> drain() {
    if (buffer.isEmpty()) {
      return Collections.emptyList();
    }
    ArrayList<Message> result = new ArrayList<>(buffer);
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
