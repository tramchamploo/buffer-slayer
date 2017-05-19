package io.bufferslayer;

import fi.iki.elonen.NanoHTTPD;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tramchamploo on 2017/3/16.
 * A simple metrics viewer on port 15090
 */
public class HttpReporterMetricsExporter extends ReporterMetricsExporter {

  private static final Logger logger = LoggerFactory.getLogger(HttpReporterMetricsExporter.class);
  static final int PORT = 15090;

  private class HttpServer extends NanoHTTPD {

    private final ReporterMetrics metrics;

    HttpServer(ReporterMetrics metrics) throws IOException {
      super(PORT);
      this.metrics = metrics;
      start(NanoHTTPD.SOCKET_READ_TIMEOUT);
      logger.info("Metrics server started at: " + PORT);
    }

    @Override
    public Response serve(IHTTPSession session) {
      String msg = "{"
          + "\"messages\":" + metrics.messages() + ","
          + "\"messagesDropped\":" + metrics.messagesDropped() + ","
          + "\"queuedMessages\":" + metrics.queuedMessages()
          + "}";
      return newFixedLengthResponse(NanoHTTPD.Response.Status.OK, "application/json", msg);
    }
  }

  private HttpServer server;

  @Override
  public void start(ReporterMetrics metrics) {
    try {
      server = new HttpServer(metrics);
    } catch (IOException e) {
      logger.warn("Exporting server failed to start. ", e);
    }
  }

  @Override
  public void close() {
    if (server != null) server.stop();
  }
}
