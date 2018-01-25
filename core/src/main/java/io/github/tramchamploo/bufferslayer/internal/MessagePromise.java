/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.github.tramchamploo.bufferslayer.internal;

import io.github.tramchamploo.bufferslayer.Message;

/**
 * Special {@link MessageFuture} which is writable.
 */
public interface MessagePromise<V> extends MessageFuture<V>, Promise<V> {

    @Override
    Message message();

    @Override
    MessagePromise<V> setSuccess(V result);

    MessagePromise<V> setSuccess();

    boolean trySuccess();

    @Override
    MessagePromise<V> setFailure(Throwable cause);

    @Override
    MessagePromise<V> addListener(GenericFutureListener<? extends Future<? super V>> listener);

    @Override
    MessagePromise<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners);

    @Override
    MessagePromise<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener);

    @Override
    MessagePromise<V> removeListeners(
        GenericFutureListener<? extends Future<? super V>>... listeners);

    @Override
    MessagePromise<V> sync() throws InterruptedException;

    @Override
    MessagePromise<V> syncUninterruptibly();

    @Override
    MessagePromise<V> await() throws InterruptedException;

    @Override
    MessagePromise<V> awaitUninterruptibly();
}
