package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.Component;
import java.util.List;
import org.jdeferred.Promise;

/**
 * Created by tramchamploo on 2017/4/14.
 */
public interface AsyncSender<M extends Message, R> extends Component {

  /**
   * asynchronously send messages
   *
   * @param messages messages to send
   * @return promise represents the result of sending
   */
  Promise<List<R>, MessageDroppedException, ?> send(List<M> messages);
}
