package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.Deferreds;
import io.github.tramchamploo.bufferslayer.internal.SendingTask;
import io.reactivex.functions.Consumer;
import java.util.List;
import org.jdeferred.Deferred;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SenderConsumerBridge {

  private static final Logger logger = LoggerFactory.getLogger(SenderConsumerBridge.class);

  /**
   * Adapt a {@link Sender} to rx-java's {@link Consumer}
   */
  @SuppressWarnings("unchecked")
  static <M extends Message, R> Consumer<List<SendingTask<M>>> toConsumer(final Sender<M, R> sender) {
    return new Consumer<List<SendingTask<M>>>() {
      @Override
      public void accept(final List<SendingTask<M>> tasks) throws Exception {
        if (tasks.isEmpty()) {
          return;
        }
        logger.debug("Sending {} messages.", tasks.size());

        Object[] messageAndDeferred = SendingTask.unzipGeneric(tasks);
        final List<M> messages = (List<M>) messageAndDeferred[0];
        final List<Deferred> deferreds = (List<Deferred>) messageAndDeferred[1];

        if (sender instanceof SyncSender) {
          SyncSender<M, R> syncSender = (SyncSender<M, R>) sender;

          try {
            List<R> result = syncSender.send(messages);
            Deferreds.resolveAll(result, deferreds);
          } catch (Throwable t) {
            Deferreds.rejectAll(MessageDroppedException.dropped(t, messages), deferreds, messages);
          }
        } else {
          AsyncSender<M, R> asyncSender = (AsyncSender<M, R>) sender;
          asyncSender.send(tasks);
        }
      }
    };
  }

  private SenderConsumerBridge() {}
}
