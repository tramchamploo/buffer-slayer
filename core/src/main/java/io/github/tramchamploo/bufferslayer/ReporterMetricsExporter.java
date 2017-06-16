package io.github.tramchamploo.bufferslayer;

import java.io.Closeable;

/**
 * Created by tramchamploo on 2017/3/16.
 */
public abstract class ReporterMetricsExporter implements Closeable {

  public abstract void start(ReporterMetrics metrics);

  public static ReporterMetricsExporter of(String type) {
    switch (type.toLowerCase()) {
      case "http":
        return new HttpReporterMetricsExporter();
      case "log":
        return new LogReporterMetricsExporter();
      default:
        return NOOP_EXPORTER;
    }
  }

  @Override
  public abstract void close();

  static final ReporterMetricsExporter NOOP_EXPORTER = new ReporterMetricsExporter() {
    @Override
    public void start(ReporterMetrics metrics) {
    }

    public void close() {
    }
  };
}
