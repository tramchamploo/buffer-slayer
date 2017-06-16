package io.github.tramchamploo.bufferslayer;

import org.jdeferred.Promise;

/**
 * Created by tramchamploo on 2017/2/27.
 */
public interface Reporter<M extends Message, R> {

  /**
   * Schedules the message to be sent onto the transport.
   *
   * @param message Message, should not be <code>null</code>.
   */
  Promise<R, MessageDroppedException, ?> report(M message);
}
