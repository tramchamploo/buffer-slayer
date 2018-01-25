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
 * The {@link CompleteMessageFuture} which is succeeded already.
 */
public final class SucceededMessageFuture<V> extends CompleteMessageFuture<V> {

    /**
     * Creates a new instance.
     *
     * @param message the {@link Message} associated with this future
     */
    public SucceededMessageFuture(Message message, Executor executor) {
        super(message, executor);
    }

    @Override
    public Throwable cause() {
        return null;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }
}
