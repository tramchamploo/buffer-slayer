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
 * The default {@link MessagePromise} implementation.
 */
public class DefaultMessagePromise<V> extends DefaultPromise<V> implements MessagePromise<V> {

    private final Message message;

    /**
     * Creates a new instance.
     *
     * @param message
     *        the {@link Message} associated with this future
     */
    public DefaultMessagePromise(Message message) {
        this.message = message;
    }

    /**
     * Creates a new instance.
     *
     * @param message
     *        the {@link Message} associated with this future
     */
    public DefaultMessagePromise(Message message, Executor executor) {
        super(executor);
        this.message = message;
    }

    @Override
    public Message message() {
        return message;
    }

    @Override
    public MessagePromise<V> setSuccess() {
        return setSuccess(null);
    }

    @Override
    public MessagePromise<V> setSuccess(V result) {
        super.setSuccess(result);
        return this;
    }

    @Override
    public boolean trySuccess() {
        return trySuccess(null);
    }

    @Override
    public MessagePromise<V> setFailure(Throwable cause) {
        super.setFailure(cause);
        return this;
    }

    @Override
    public MessagePromise<V> addListener(GenericFutureListener<? extends Future<? super V>> listener) {
        super.addListener(listener);
        return this;
    }

    @Override
    public MessagePromise<V> addListeners(GenericFutureListener<? extends Future<? super V>>... listeners) {
        super.addListeners(listeners);
        return this;
    }

    @Override
    public MessagePromise<V> removeListener(GenericFutureListener<? extends Future<? super V>> listener) {
        super.removeListener(listener);
        return this;
    }

    @Override
    public MessagePromise<V> removeListeners(GenericFutureListener<? extends Future<? super V>>... listeners) {
        super.removeListeners(listeners);
        return this;
    }

    @Override
    public MessagePromise<V> sync() throws InterruptedException {
        super.sync();
        return this;
    }

    @Override
    public MessagePromise<V> syncUninterruptibly() {
        super.syncUninterruptibly();
        return this;
    }

    @Override
    public MessagePromise<V> await() throws InterruptedException {
        super.await();
        return this;
    }

    @Override
    public MessagePromise<V> awaitUninterruptibly() {
        super.awaitUninterruptibly();
        return this;
    }
}
