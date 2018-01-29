package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.Component;
import io.github.tramchamploo.bufferslayer.internal.CompositeFuture;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import java.util.List;

/**
 * Sender that send messages in async way and return a promise
 */
public interface AsyncSender<R> extends Component {

  /**
   * asynchronously send messages
   *
   * @param promises messages to send
   */
  CompositeFuture send(List<MessagePromise<R>> promises);
}
