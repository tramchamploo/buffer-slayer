package io.bufferslayer;

import java.io.Closeable;

/**
 * Created by guohang.bao on 2017/3/16.
 */
public abstract class ReporterMetricsExporter implements Closeable {

  public abstract void start(ReporterMetrics metrics);

  public enum Exporters {
    http, log, noop
  }

  public static ReporterMetricsExporter of(Exporters type) {
    if (type == Exporters.http) {
      return new HttpReporterMetricsExporter();
    } else if (type == Exporters.log) {
      return new LogReporterMetricsExporter();
    }
    return NOOP_EXPORTER;
  }

  @Override
  public abstract void close();

  public static final ReporterMetricsExporter NOOP_EXPORTER = new ReporterMetricsExporter() {
    @Override
    public void start(ReporterMetrics metrics) {
    }

    public void close() {
    }
  };
}
