package io.github.tramchamploo.bufferslayer;

import java.util.List;

/**
 * @param <M> Type of Message
 * @param <R> Type of return value
 */
public interface SyncSender<M extends Message, R> extends Sender<M, R> {

  /**
   * blocking send messages
   *
   * @param messages messages to be executed
   */
  List<R> send(List<M> messages);
}
