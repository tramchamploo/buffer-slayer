package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.TestUtil.propertyOr;
import static io.github.tramchamploo.bufferslayer.TestUtil.randomString;

import com.github.mauricio.async.db.Configuration;
import com.github.mauricio.async.db.mysql.util.URLParser;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public abstract class AbstractVertxTimeUsedComparison extends AbstractTimeUsedComparison {

  private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS test";
  private static final String CREATE_TABLE = "CREATE TABLE test.benchmark(id INT PRIMARY KEY AUTO_INCREMENT, data VARCHAR(32), time TIMESTAMP)";
  private static final String DROP_TABLE = "DROP TABLE IF EXISTS test.benchmark";
  private static final String TRUNCATE_TABLE = "TRUNCATE TABLE test.benchmark";
  private static final String INSERTION = "INSERT INTO test.benchmark(data, time) VALUES(?, now())";
  private static final String MODIFICATION = "UPDATE test.benchmark SET data = ? WHERE id = ?";

  private static final int ITERATIONS = 10;
  private static final int WARMUPS = 3;

  private static final Configuration jdbcUrl = URLParser.parse(propertyOr("jdbcUrl", "jdbc:mysql://127.0.0.1:3306?useSSL=false"), Charset.forName("UTF-8"));
  private static final JsonObject mySQLClientConfig = new JsonObject()
      .put("host", jdbcUrl.host())
      .put("port", jdbcUrl.port())
      .put("username", propertyOr("username", "root"))
      .put("password", propertyOr("password", "root"))
      .put("database", "");

  protected void run() throws Exception {
    Vertx vertx = Vertx.vertx();
    AsyncSQLClient unbatch = MySQLClient.createShared(vertx, mySQLClientConfig);

    final CountDownLatch countDown = new CountDownLatch(1);

    unbatch.getConnection(connection -> {
      SQLConnection conn = connection.result();
      conn.update(CREATE_DATABASE, cd -> {
        System.out.println("CREATE DATABASE: " + cd.succeeded());

        conn.update(DROP_TABLE, dt -> {
          System.out.println("DROP TABLE: " + dt.succeeded());

          conn.update(CREATE_TABLE, ct -> {
            System.out.println("CREATE TABLE: " + ct.succeeded());
            conn.close();
            countDown.countDown();
          });
        });
      });
    });
    countDown.await();

    avgTime(WARMUPS, unbatch, this::measureTimeInNanos);

    long avgTime = (long) avgTime(ITERATIONS, unbatch, this::measureTimeInNanos);
    System.out.println("unbatched time used: " + avgTime);

    SQLConnectionSender sender = new SQLConnectionSender(unbatch);

    Reporter<Statement, UpdateResult> reporter = reporter(sender);
    AsyncSQLClient batch = BatchMySQLClient.wrap(vertx, unbatch, reporter);
    avgTime = (long) avgTime(ITERATIONS, batch, this::measureTimeInNanos);
    System.out.println("batched time used: " + avgTime);

    vertx.close();
  }

  private void truncateTable(AsyncSQLClient client) {
    final CountDownLatch countDown = new CountDownLatch(1);

    client.getConnection(connection -> {
      SQLConnection conn = connection.result();
      conn.update(TRUNCATE_TABLE, tt -> {
        System.out.println("TRUNCATE TABLE: " + tt.succeeded());
        conn.close();
        countDown.countDown();
      });
    });
    try {
      countDown.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private double avgTime(int iterations, AsyncSQLClient client, Function<AsyncSQLClient, Long> measurement) {
    List<Long> times = new ArrayList<>();
    for (int i = 0; i < iterations; i++) {
      truncateTable(client);
      times.add(measurement.apply(client));
    }
    return times.stream().mapToLong(value -> value).average().getAsDouble();
  }

  private long measureTimeInNanos(AsyncSQLClient client) {
    CountDownLatch countDown = new CountDownLatch(1);
    AtomicLong counter = new AtomicLong();
    Random random = new Random(System.currentTimeMillis());

    long start = System.nanoTime();

    for (int i = 0; i < 5000; i++) {
      int finalI = i;
      client.getConnection(connection -> {
        SQLConnection conn = connection.result();

        conn.updateWithParams(INSERTION,
            new JsonArray().add(randomString()), update -> {

          if (update.succeeded()) {
            long count = counter.addAndGet(update.result().getUpdated());
            if (count == 5050) countDown.countDown();

            if (finalI % 100 == 0) {
              conn.updateWithParams(MODIFICATION,
                  new JsonArray().add(randomString()).add(random.nextInt(finalI + 1) + 1), mod -> {
                conn.close();

                if (update.succeeded()) {
                  long ccount = counter.addAndGet(mod.result().getUpdated());
                  if (ccount == 5050) countDown.countDown();
                }
              });
            } else {
              conn.close();
            }
          }
        });
      });
    }
    try {
      countDown.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return System.nanoTime() - start;
  }
}
