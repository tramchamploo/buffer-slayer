package io.bufferslayer;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import org.jdeferred.Deferred;
import org.jdeferred.DeferredManager;
import org.jdeferred.Promise;
import org.jdeferred.impl.DefaultDeferredManager;
import org.jdeferred.impl.DeferredObject;
import org.jdeferred.multiple.MasterProgress;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneReject;

/**
 * Created by tramchamploo on 2017/4/14.
 */
abstract class SenderToAsyncSenderAdaptor<M extends Message, R> implements AsyncSender<M> {

  final Sender<M, R> delegate;
  final Executor executor;
  final DeferredManager deferredManager = new DefaultDeferredManager();

  SenderToAsyncSenderAdaptor(Sender<M, R> delegate, Executor executor) {
    this.delegate = checkNotNull(delegate);
    this.executor = checkNotNull(executor);
  }

  /**
   * Split messages into pieces so that they can be sent in parallel
   *
   * @param messages messages to split
   * @return splitted messages
   */
  protected abstract List<List<M>> split(List<M> messages);

  @Override
  public Promise<MultipleResults, OneReject, MasterProgress> send(List<M> messages) {
    List<List<M>> batches = split(messages);
    List<Promise<List<R>, MessageDroppedException, ?>> promises =
        new ArrayList<>(batches.size());

    for (final List<M> batch : batches) {
      final Deferred<List<R>, MessageDroppedException, ?> deferred = new DeferredObject<>();
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            deferred.resolve(delegate.send(batch));
          } catch (Throwable t) {
            deferred.reject(MessageDroppedException.dropped(t, batch));
          }
        }
      });
      promises.add(deferred.promise());
    }

    return deferredManager.when(promises.toArray(new Promise[0]));
  }

  @Override
  public CheckResult check() {
    return delegate.check();
  }

  @Override
  public void close() throws IOException {
    if (executor instanceof ExecutorService) {
      ((ExecutorService) executor).shutdown();
    }
    delegate.close();
  }
}
