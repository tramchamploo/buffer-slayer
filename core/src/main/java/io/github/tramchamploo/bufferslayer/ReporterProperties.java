package io.github.tramchamploo.bufferslayer;

public interface ReporterProperties {

  /**
   * Construct a {@link io.github.tramchamploo.bufferslayer.Reporter.Builder} using properties
   */
  <M extends Message, R> Reporter.Builder<M, R> toBuilder(Sender<M, R> sender);
}
