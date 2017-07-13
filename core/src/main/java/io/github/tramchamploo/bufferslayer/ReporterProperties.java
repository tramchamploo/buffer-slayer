package io.github.tramchamploo.bufferslayer;

public interface ReporterProperties<T extends ReporterProperties> {

  /**
   * Set the sender for reporter
   */
  T setSender(Sender<? extends Message, ?> sender);

  /**
   * Construct a {@link io.github.tramchamploo.bufferslayer.Reporter.Builder} using properties
   */
  Reporter.Builder toBuilder();
}
