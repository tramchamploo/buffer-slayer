package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.Component;

/**
 * Base interface for both sync and async sender
 */
public interface Sender<M extends Message, R> extends Component {
}
