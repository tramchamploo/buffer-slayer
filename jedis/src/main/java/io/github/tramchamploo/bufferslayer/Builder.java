package io.github.tramchamploo.bufferslayer;

/**
 * Builder that build response to type T
 * @param <T> the result type
 */
abstract class Builder<T> {

  abstract T build(Object data);
}
