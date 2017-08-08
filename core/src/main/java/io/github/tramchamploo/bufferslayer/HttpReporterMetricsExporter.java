package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.internal.Properties.propertyOr;

import fi.iki.elonen.NanoHTTPD;
import java.io.IOException;
import java.net.ServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tramchamploo on 2017/3/16.
 * A simple metrics viewer on port 15090.
 * If port already in use, pick a unused one randomly.
 */
public class HttpReporterMetricsExporter extends ReporterMetricsExporter {

  private static final Logger logger = LoggerFactory.getLogger(HttpReporterMetricsExporter.class);
  static final int PORT = propertyOr("bufferslayer.metrics.http.port", 15090);

  private class HttpServer extends NanoHTTPD {

    private final ReporterMetrics metrics;

    HttpServer(int port, ReporterMetrics metrics) throws IOException {
      super(port);
      this.metrics = metrics;
      start(NanoHTTPD.SOCKET_READ_TIMEOUT);
      logger.info("Metrics server started at: " + port);
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
      server = new HttpServer(PORT, metrics);
    } catch (IOException e) {
      logger.warn("Port {} already in use, pick another one.", PORT);
      try {
        server = new HttpServer(pickUnusedPort(), metrics);
      } catch (IOException e2) {
        logger.warn("Exporting server failed to start. ", e2);
      }
    }
  }

  private int pickUnusedPort() {
    try {
      ServerSocket serverSocket = new ServerSocket(0);
      int port = serverSocket.getLocalPort();
      serverSocket.close();
      return port;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (server != null) server.stop();
  }
}
