package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.TestMessage.newMessage;

import io.github.tramchamploo.bufferslayer.internal.SendingTask;
import org.jdeferred.impl.DeferredObject;

class Util {

  static SendingTask<TestMessage> newSendingTask(int key) {
    return new SendingTask<>(newMessage(key), new DeferredObject<>());
  }

  private Util() {}
}
