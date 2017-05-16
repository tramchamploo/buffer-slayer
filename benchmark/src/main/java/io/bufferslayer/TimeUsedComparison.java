package io.bufferslayer;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Created by tramchamploo on 2017/3/30.
 */
public class TimeUsedComparison {

  static String randomString() {
    return String.valueOf(ThreadLocalRandom.current().nextLong());
  }

  static String propertyOr(String key, String fallback) {
    return System.getProperty(key, fallback);
  }

  public static void main(String[] args) throws Exception {
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

    AsyncReporter reporter = AsyncReporter.builder(proxy)
        .pendingMaxMessages(6000)
        .bufferedMaxMessages(100)
        .messageTimeout(50, TimeUnit.MILLISECONDS)
        .pendingKeepalive(10, TimeUnit.MILLISECONDS)
        .senderExecutor(Executors.newCachedThreadPool())
        .parallelismPerBatch(10)
        .strictOrder(true)
        .build();
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
      batch.update(INSERTION, new Object[] {randomString(), new Date()});
      if (i % 100 == 0) {
        batch.update(MODIFICATION, new Object[] {randomString(), random.nextInt(i + 1) + 1});
      }
    }
    countDown.await();
    long used = System.nanoTime() - start;
    System.out.println("batch time used: " + used);
    reporter.sender.close();
    reporter.close();

    unbatch.update(DROP_TABLE);
    unbatch.update(CREATE_TABLE);
    start = System.nanoTime();
    for (int i = 0; i < 5000; i++) {
      unbatch.update(INSERTION, new Object[] {randomString(), new Date()});
      if (i % 100 == 0) {
        unbatch.update(MODIFICATION, new Object[] {randomString(), random.nextInt(i + 1) + 1});
      }
    }
    used = System.nanoTime() - start;
    System.out.println("unbatch time used: " + used);
    unbatch.update(DROP_TABLE);
  }
}
