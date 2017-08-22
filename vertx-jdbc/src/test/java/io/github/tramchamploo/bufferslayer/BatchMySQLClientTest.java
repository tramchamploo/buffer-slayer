package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.TestUtil.envOr;
import static io.github.tramchamploo.bufferslayer.TestUtil.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.mauricio.async.db.Configuration;
import com.github.mauricio.async.db.mysql.util.URLParser;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class BatchMySQLClientTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS test.test(id INT PRIMARY KEY AUTO_INCREMENT, data VARCHAR(32), time TIMESTAMP)";
  private static final String TRUNCATE_TABLE = "TRUNCATE TABLE test.test";
  private static final String INSERTION = "INSERT INTO test.test(data, time) VALUES(?, now());";
  private static final String ROW_COUNT = "SELECT COUNT(1) AS count FROM test.test;";

  private static final Configuration jdbcUrl =
      URLParser.parse(envOr("MYSQL_URL", "jdbc:mysql://127.0.0.1:3306?useSSL=false"), Charset.forName("UTF-8"));
  private static final JsonObject mySQLClientConfig = new JsonObject()
      .put("host", jdbcUrl.host())
      .put("port", jdbcUrl.port())
      .put("username", envOr("MYSQL_USERNAME", "root"))
      .put("password", envOr("MYSQL_PASSWORD", "root"))
      .put("database", "");

  private static Vertx vertx = Vertx.vertx();
  private AsyncSQLClient unbatch = MySQLClient.createShared(vertx, mySQLClientConfig);
  private AsyncSQLClient batch;

  private AsyncReporter<Statement, UpdateResult> reporter;

  @AfterClass
  public static void destory() {
    vertx.close();
  }

  @Before
  public void setup() throws InterruptedException {
    final CountDownLatch countDown = new CountDownLatch(1);

    unbatch.getConnection(connection -> {
      SQLConnection conn = connection.result();
      conn.update(CREATE_TABLE, ct -> {
        System.out.println("CREATE TABLE: " + ct.succeeded());

        conn.update(TRUNCATE_TABLE, tt -> {
          System.out.println("TRUNCATE TABLE: " + tt.succeeded());
            conn.close();
            countDown.countDown();
        });
      });
    });
    countDown.await(1, TimeUnit.SECONDS);
  }

  @After
  public void clean() {
    reporter.flush();
    reporter.close();
  }

  @Test
  public void bufferedUpdate() throws Exception {
    SQLConnectionSender sender = new SQLConnectionSender(unbatch);

    reporter = AsyncReporter.builder(sender)
        .pendingMaxMessages(100)
        .bufferedMaxMessages(10)
        .messageTimeout(10, TimeUnit.MILLISECONDS)
        .build();
    batch = BatchMySQLClient.wrap(vertx, unbatch, reporter);

    AtomicInteger count = new AtomicInteger();

    CountDownLatch countDown = new CountDownLatch(100);

    for (int i = 0; i < 100; i++) {
      batch.getConnection(get -> {
        SQLConnection conn = get.result();
        conn.updateWithParams(INSERTION,
          new JsonArray().add(randomString()), update -> {
            count.addAndGet(update.result().getUpdated());
            conn.close();
            countDown.countDown();
          });
      });
    }

    countDown.await(5, TimeUnit.SECONDS);

    assertEquals(100, count.get());

    CountDownLatch countDown2 = new CountDownLatch(1);
    unbatch.getConnection(get -> {
      SQLConnection conn = get.result();
      conn.query(ROW_COUNT, query -> {
        ResultSet rs = query.result();
        int rowCount = rs.getRows().get(0).getInteger("count");
        assertEquals(100, rowCount);

        conn.close();
        countDown2.countDown();
      });
    });

    countDown2.await(5, TimeUnit.SECONDS);
    sender.close();
  }


  @Test
  public void statementFailed() throws Exception {
    FakeAsyncSender sender = new FakeAsyncSender(new SQLConnectionSender(unbatch));
    RuntimeException ex = new RuntimeException();
    sender.failed(() -> ex);

    reporter = AsyncReporter.builder(sender).messageTimeout(10, TimeUnit.MILLISECONDS).build();
    batch = BatchMySQLClient.wrap(vertx, unbatch, reporter);

    CountDownLatch countDown = new CountDownLatch(1);
    batch.getConnection(get -> {
      SQLConnection conn = get.result();
      conn.updateWithParams(INSERTION,
        new JsonArray().add(randomString()), update -> {
          assertTrue(update.failed());
          assertEquals(ex, update.cause().getCause());

          conn.close();
          countDown.countDown();
        });
    });
    countDown.await(500, TimeUnit.MILLISECONDS);
    sender.close();
  }
}
