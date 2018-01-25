package io.github.tramchamploo.bufferslayer.internal;

import io.github.tramchamploo.bufferslayer.Message;

public interface MessageFuture<V> extends Future<V> {

  /**
   * Returns a message where the I/O operation associated with this
   * future takes place.
   */
  Message message();

  @Override
  MessageFuture<V> addListener(GenericFutureListener<? extends Future<? super V>> listener);

  @Override
  MessageFuture<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

  @Override
  MessageFuture<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener);

  @Override
  MessageFuture<V> removeListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

  @Override
  MessageFuture<V> sync() throws InterruptedException;

  @Override
  MessageFuture<V> syncUninterruptibly();

  @Override
  MessageFuture<V> await() throws InterruptedException;

  @Override
  MessageFuture<V> awaitUninterruptibly();
}
