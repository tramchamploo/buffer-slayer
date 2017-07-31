package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.TestUtil.propertyOr;
import static io.github.tramchamploo.bufferslayer.TestUtil.randomString;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Compare time used for batched and non batched sql updates.
 */
public abstract class AbstractTimeUsedComparison {

  protected void run() throws Exception {
    BatchJdbcTemplate batch;
    JdbcTemplate unbatch;
    SenderProxy proxy;

    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName("com.mysql.jdbc.Driver");
    dataSource.setUrl(propertyOr("jdbcUrl", "jdbc:mysql://127.0.0.1:3306?useSSL=false&rewriteBatchedStatements=true"));
    dataSource.setUsername(propertyOr("username", "root"));
    dataSource.setPassword(propertyOr("password", "root"));

    JdbcTemplate delegate = new JdbcTemplate(dataSource);
    delegate.setDataSource(dataSource);

    AtomicLong counter = new AtomicLong();

    final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS test";
    final String CREATE_TABLE = "CREATE TABLE test.benchmark(id INT PRIMARY KEY AUTO_INCREMENT, data VARCHAR(32), time TIMESTAMP)";
    final String DROP_TABLE = "DROP TABLE IF EXISTS test.benchmark";
    final String INSERTION = "INSERT INTO test.benchmark(data, time) VALUES(?, ?)";
    final String MODIFICATION = "UPDATE test.benchmark SET data = ? WHERE id = ?";

    CountDownLatch countDown = new CountDownLatch(1);

    proxy = new SenderProxy(new JdbcTemplateSender(delegate));
    proxy.onMessages(updated -> {
      if (counter.addAndGet(updated.size()) == 5050) {
        countDown.countDown();
      }
    });

    Reporter<SQL, Integer> reporter = reporter(proxy);
    batch = new BatchJdbcTemplate(delegate, reporter);
    batch.setDataSource(dataSource);

    unbatch = new JdbcTemplate(dataSource);
    unbatch.setDataSource(dataSource);

    unbatch.update(CREATE_DATABASE);
    unbatch.update(DROP_TABLE);
    unbatch.update(CREATE_TABLE);

    Random random = new Random(System.currentTimeMillis());

    long start = System.nanoTime();
    for (int i = 0; i < 5000; i++) {
      batch.update(INSERTION, randomString(), new Date());
      if (i % 100 == 0) {
        batch.update(MODIFICATION, randomString(), random.nextInt(i + 1) + 1);
      }
    }
    countDown.await();
    long used = System.nanoTime() - start;
    System.out.println("batch time used:   " + used);
    reporter.close();

    unbatch.update(DROP_TABLE);
    unbatch.update(CREATE_TABLE);
    start = System.nanoTime();
    for (int i = 0; i < 5000; i++) {
      unbatch.update(INSERTION, randomString(), new Date());
      if (i % 100 == 0) {
        unbatch.update(MODIFICATION, randomString(), random.nextInt(i + 1) + 1);
      }
    }
    used = System.nanoTime() - start;
    System.out.println("unbatch time used: " + used);
    unbatch.update(DROP_TABLE);
  }

  protected abstract <S extends Message, R> Reporter<S, R> reporter(Sender<S, R> actual);
}
