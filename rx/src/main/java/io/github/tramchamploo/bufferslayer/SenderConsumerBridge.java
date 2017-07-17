package io.github.tramchamploo.bufferslayer;

import io.reactivex.functions.Consumer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SenderConsumerBridge {

  private static final Logger logger = LoggerFactory.getLogger(SenderConsumerBridge.class);

  /**
   * Adapt a {@link Sender} to rx-java's {@link Consumer}
   */
  static <M extends Message, R> Consumer<List<M>> toConsumer(final Sender<M, R> sender) {
    return new Consumer<List<M>>() {
      @Override
      public void accept(List<M> messages) throws Exception {
        if (messages.isEmpty()) return;
        logger.debug("Sending {} messages.", messages.size());
        try {
          List<R> result = sender.send(messages);
          DeferredHolder.batchResolve(messages, result);
        } catch (Throwable t) {
          DeferredHolder.batchReject(messages, MessageDroppedException.dropped(t, messages));
        }
      }
    };
  }
}
