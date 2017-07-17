package io.github.tramchamploo.bufferslayer;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Created by tramchamploo on 2017/3/30.
 */
public abstract class AbstractTimeUsedComparison {

  static String randomString() {
    return String.valueOf(ThreadLocalRandom.current().nextLong());
  }

  static String propertyOr(String key, String fallback) {
    return System.getProperty(key, fallback);
  }

  protected void run() throws Exception {
    BatchJdbcTemplate batch;
    JdbcTemplate unbatch;
    SenderProxy proxy;

    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
    dataSource.setUrl(propertyOr("jdbcUrl", "jdbc:mysql://127.0.0.1:3306?useSSL=false"));
    dataSource.setUsername(propertyOr("username", "root"));
    dataSource.setPassword(propertyOr("password", "root"));

    JdbcTemplate delegate = new JdbcTemplate(dataSource);
    delegate.setDataSource(dataSource);

    AtomicLong counter = new AtomicLong();

    final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS test";
    final String CREATE_TABLE = "CREATE TABLE test.benchmark(id INT PRIMARY KEY AUTO_INCREMENT, data VARCHAR(32), time TIMESTAMP);";
    final String DROP_TABLE = "DROP TABLE IF EXISTS test.benchmark;";
    final String INSERTION = "INSERT INTO test.benchmark(data, time) VALUES(?, ?);";
    final String MODIFICATION = "UPDATE test.benchmark SET data = ? WHERE id = ?;";

    CountDownLatch countDown = new CountDownLatch(1);

    proxy = new SenderProxy(new JdbcTemplateSender(delegate));
    proxy.onMessages(updated -> {
      if (counter.addAndGet(updated.size()) == 5050) {
        countDown.countDown();
      }
    });

    Reporter<Sql, Integer> reporter = getReporter(proxy);
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

  protected abstract Reporter<Sql, Integer> getReporter(Sender<Sql, Integer> actual);
}
