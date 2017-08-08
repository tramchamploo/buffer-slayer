package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.Component;
import io.github.tramchamploo.bufferslayer.internal.SendingTask;
import java.util.List;
import org.jdeferred.Promise;
import org.jdeferred.multiple.MasterProgress;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneReject;

/**
 * Sender that send messages in async way and return a promise
 */
public interface AsyncSender<M extends Message> extends Component {

  /**
   * asynchronously send messages
   *
   * @param messages messages to send
   */
  Promise<MultipleResults, OneReject, MasterProgress> send(List<SendingTask<M>> messages);
}
