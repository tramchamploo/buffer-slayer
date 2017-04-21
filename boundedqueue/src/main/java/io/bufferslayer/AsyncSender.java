package io.bufferslayer;

import io.bufferslayer.internal.Component;
import java.util.List;
import org.jdeferred.Promise;
import org.jdeferred.multiple.MasterProgress;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneReject;

/**
 * Created by guohang.bao on 2017/4/14.
 */
public interface AsyncSender<M extends Message> extends Component {

  /**
   * asynchronously send messages
   *
   * @param messages messages to send
   * @return promise represents the result dropped sending
   */
  Promise<MultipleResults, OneReject, MasterProgress> send(List<M> messages);
}
