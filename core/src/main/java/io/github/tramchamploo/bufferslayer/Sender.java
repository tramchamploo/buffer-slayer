package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.Component;
import java.util.List;

/**
 * Send that send batched messages
 * @param <M> Type of Message
 * @param <R> Type of return value
 */
public interface Sender<M extends Message, R> extends Component {

  /**
   * blocking send messages
   *
   * @param messages messages to be sent
   */
  List<R> send(List<M> messages);
}
