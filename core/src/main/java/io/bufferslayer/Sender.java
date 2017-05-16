package io.bufferslayer;

import io.bufferslayer.internal.Component;
import java.util.List;

/**
 * Created by tramchamploo on 2017/2/27.
 * @param <M> Type of Message
 * @param <R> Type of return value
 */
public interface Sender<M extends Message, R> extends Component {

  /**
   * blocking send messages
   *
   * @param messages messages to be executed
   */
  List<R> send(List<M> messages);
}
