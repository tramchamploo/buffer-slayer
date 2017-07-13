package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.RxReporter.Builder;

import java.util.concurrent.TimeUnit;

public class RxReporterProperties extends AbstractReporterProperties<RxReporterProperties> {

  public Builder toBuilder() {
    Builder builder = new Builder<>(sender).messageTimeout(messageTimeoutNanos, TimeUnit.NANOSECONDS)
                                           .bufferedMaxMessages(bufferedMaxMessages)
                                           .pendingMaxMessages(pendingMaxMessages)
                                           .overflowStrategy(OverflowStrategy.create(overflowStrategy));

    if (metrics.equalsIgnoreCase("inmemory")) {
      builder.metrics(InMemoryReporterMetrics.instance(ReporterMetricsExporter.of(metricsExporter)));
    }
    return builder;
  }
}
