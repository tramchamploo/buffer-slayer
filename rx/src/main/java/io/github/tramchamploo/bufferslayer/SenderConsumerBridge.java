package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
import io.github.tramchamploo.bufferslayer.internal.Promises;
import io.reactivex.functions.Consumer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SenderConsumerBridge {

  private static final Logger logger = LoggerFactory.getLogger(SenderConsumerBridge.class);

  /**
   * Adapt a {@link Sender} to rx-java's {@link Consumer}
   */
  static <M extends Message, R> Consumer<List<MessagePromise<R>>> toConsumer(final Sender<M, R> sender) {
    return new Consumer<List<MessagePromise<R>>>() {
      @Override
      public void accept(List<MessagePromise<R>> promises) {
        if (promises.isEmpty()) {
          return;
        }
        logger.debug("Sending {} messages.", promises.size());

        List<M> messages = extractMessages(promises);
        try {
          List<R> result = sender.send(messages);
          Promises.allSuccess(result, promises);
        } catch (Throwable t) {
          Promises.allFail(MessageDroppedException.dropped(t, messages), promises, messages);
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <M, R> List<M> extractMessages(List<MessagePromise<R>> promises) {
    List<M> messages = new ArrayList<>();
    for (MessagePromise<R> promise : promises) {
      messages.add((M) promise.message());
    }
    return messages;
  }


  private SenderConsumerBridge() {
  }
}
