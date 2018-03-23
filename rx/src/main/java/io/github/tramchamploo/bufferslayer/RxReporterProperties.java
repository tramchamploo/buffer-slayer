package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.RxReporter.Builder;
import java.util.concurrent.TimeUnit;

public class RxReporterProperties extends AbstractReporterProperties {

  @Override
  public RxReporterProperties setMetrics(String metrics) {
    super.setMetrics(metrics);
    return this;
  }

  @Override
  public RxReporterProperties setMessageTimeout(long messageTimeout, TimeUnit unit) {
    super.setMessageTimeout(messageTimeout, unit);
    return this;
  }

  @Override
  public RxReporterProperties setBufferedMaxMessages(int bufferedMaxMessages) {
    super.setBufferedMaxMessages(bufferedMaxMessages);
    return this;
  }

  @Override
  public RxReporterProperties setPendingMaxMessages(int pendingMaxMessages) {
    super.setPendingMaxMessages(pendingMaxMessages);
    return this;
  }

  @Override
  public RxReporterProperties setMetricsExporter(String exporter) {
    super.setMetricsExporter(exporter);
    return this;
  }

  @Override
  public RxReporterProperties setOverflowStrategy(String overflowStrategy) {
    super.setOverflowStrategy(overflowStrategy);
    return this;
  }

  @Override
  public <M extends Message, R> Builder<M, R> toBuilder(Sender<M, R> sender) {
    Builder<M, R> builder = new Builder<>(sender).messageTimeout(messageTimeoutNanos, TimeUnit.NANOSECONDS)
                                           .bufferedMaxMessages(bufferedMaxMessages)
                                           .pendingMaxMessages(pendingMaxMessages)
                                           .overflowStrategy(OverflowStrategy.create(overflowStrategy));

    if (metrics.equalsIgnoreCase("inmemory")) {
      builder.metrics(InMemoryReporterMetrics.instance(ReporterMetricsExporter.of(metricsExporter)));
    }
    return builder;
  }
}
