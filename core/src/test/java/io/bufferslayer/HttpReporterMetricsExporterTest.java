package io.bufferslayer;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Test;

/**
 * Created by tramchamploo on 2017/5/18.
 */
public class HttpReporterMetricsExporterTest {

  OkHttpClient client = new OkHttpClient();

  @Test
  public void jsonFormat() throws IOException {
    ReporterMetricsExporter exporter = new HttpReporterMetricsExporter();
    ReporterMetrics metrics = InMemoryReporterMetrics.instance(exporter);

    Request request = new Request.Builder()
        .url("http://localhost:" + HttpReporterMetricsExporter.PORT)
        .build();
    Response response = client.newCall(request).execute();

    assertEquals("{\"messages\":0,\"messagesDropped\":0,\"queuedMessages\":0}",
        response.body().string());
    assertEquals("application/json", response.header("Content-Type"));

    metrics.incrementMessages(1);
    metrics.incrementMessagesDropped(2);
    metrics.updateQueuedMessages(Message.SINGLE_KEY, 3);
    response = client.newCall(request).execute();
    assertEquals("{\"messages\":1,\"messagesDropped\":2,\"queuedMessages\":3}",
        response.body().string());
    exporter.close();
  }
}
