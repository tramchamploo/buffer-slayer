package io.bufferslayer;

import org.jdeferred.Promise;

/**
 * Created by guohang.bao on 2017/2/27.
 */
public interface Reporter {

  /**
   * Schedules the message to be sent onto the transport.
   *
   * @param message Message, should not be <code>null</code>.
   */
  Promise report(Message message);
}
