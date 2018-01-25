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
import java.util.concurrent.Executor;

/**
 * A skeletal {@link MessageFuture} implementation which represents a
 * {@link MessageFuture} which has been completed already.
 */
abstract class CompleteMessageFuture<V> extends CompleteFuture<V> implements MessageFuture<V> {

    private final Message message;

    /**
     * Creates a new instance.
     *
     * @param message the {@link Message} associated with this future
     */
    protected CompleteMessageFuture(Message message, Executor executor) {
        super(executor);
        if (message == null) {
            throw new NullPointerException("message");
        }
        this.message = message;
    }

    @Override
    public MessageFuture<V> addListener(GenericFutureListener<? extends Future<? super V>> listener) {
        super.addListener(listener);
        return this;
    }

    @Override
    public MessageFuture<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners) {
        super.addListeners(listeners);
        return this;
    }

    @Override
    public MessageFuture<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener) {
        super.removeListener(listener);
        return this;
    }

    @Override
    public MessageFuture<V> removeListeners(GenericFutureListener<? extends Future<? super V>>... listeners) {
        super.removeListeners(listeners);
        return this;
    }

    @Override
    public MessageFuture<V> syncUninterruptibly() {
        return this;
    }

    @Override
    public MessageFuture<V> sync() throws InterruptedException {
        return this;
    }

    @Override
    public MessageFuture<V> await() throws InterruptedException {
        return this;
    }

    @Override
    public MessageFuture<V> awaitUninterruptibly() {
        return this;
    }

    @Override
    public Message message() {
        return message;
    }

    @Override
    public V getNow() {
        return null;
    }
}
