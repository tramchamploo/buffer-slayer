package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.DefaultMessagePromise;
import io.github.tramchamploo.bufferslayer.internal.Future;
import io.github.tramchamploo.bufferslayer.internal.FutureListener;
import io.github.tramchamploo.bufferslayer.internal.MessageFuture;

final class ResponseUtil {

  @SuppressWarnings("unchecked")
  static <T> MessageFuture<T> transformResponse(MessageFuture<?> future, final Builder<T> builder) {
    final DefaultMessagePromise<T> promise = new DefaultMessagePromise<>(future.message());
    future.addListener(new FutureListener<Object>() {

      @Override
      public void operationComplete(Future<Object> future) throws Exception {
        if (future.isSuccess()) {
          promise.setSuccess(builder.build(future.get()));
        } else {
          promise.setFailure(future.cause());
        }
      }
    });

    return promise;
  }
}
