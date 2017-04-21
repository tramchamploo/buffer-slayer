package io.bufferslayer;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Created by guohang.bao on 2017/3/30.
 */
public class TimeUsedComparison {

  public static void main(String[] args) throws PropertyVetoException, InterruptedException {
    ComboPooledDataSource dataSource;
    BatchedJdbcTemplate batched;
    JdbcTemplate unbatched;
    SenderProxy proxy;

    dataSource = new ComboPooledDataSource();
    dataSource.setDriverClass("org.h2.Driver");
    dataSource.setMinPoolSize(10);
    dataSource.setMaxPoolSize(50);
    dataSource.setJdbcUrl("jdbc:h2:~/test");

    JdbcTemplate delegate = new JdbcTemplate(dataSource);
    delegate.setDataSource(dataSource);

    AtomicLong counter = new AtomicLong();

    final String CREATE_TABLE = "CREATE TABLE benchmark(id INT PRIMARY KEY AUTO_INCREMENT, data VARCHAR(32), time TIMESTAMP);";
    final String DROP_TABLE = "DROP TABLE IF EXISTS benchmark;";
    final String INSERTION = "INSERT INTO benchmark(data, time) VALUES(?, ?);";
    final String MODIFICATION = "UPDATE benchmark SET data = ? WHERE id = ?;";

    CountDownLatch countDown = new CountDownLatch(1);

    proxy = new SenderProxy(new JdbcTemplateSender(delegate));
    proxy.onMessages(updated -> {
      if (counter.addAndGet(updated.size()) == 500500) {
        countDown.countDown();
      }
    });

    AsyncReporter reporter = AsyncReporter.builder(proxy)
        .pendingMaxMessages(6000000)
        .bufferedMaxMessages(1000)
        .senderExecutor(Executors.newCachedThreadPool())
        .parallelismPerBatch(10)
        .strictOrder(true)
        .build();
    batched = new BatchedJdbcTemplate(delegate, reporter);
    batched.setDataSource(dataSource);

    unbatched = new JdbcTemplate(dataSource);
    unbatched.setDataSource(dataSource);
    unbatched.update(DROP_TABLE);
    unbatched.update(CREATE_TABLE);

    Random random = new Random(System.currentTimeMillis());

    long start = System.nanoTime();
    for (int i = 0; i < 500000; i++) {
      batched.update(INSERTION, new Object[] {randomString(), new Date()});
      if (i % 1000 == 0) {
        batched.update(MODIFICATION, new Object[] {randomString(), random.nextInt(i + 1) + 1});
      }
    }
    countDown.await();
    long used = System.nanoTime() - start;
    System.out.println("batched time used: " + used);

    unbatched.update(DROP_TABLE);
    unbatched.update(CREATE_TABLE);
    start = System.nanoTime();
    for (int i = 0; i < 500000; i++) {
      unbatched.update(INSERTION, new Object[] {randomString(), new Date()});
      if (i % 1000 == 0) {
        unbatched.update(MODIFICATION, new Object[] {randomString(), random.nextInt(i + 1) + 1});
      }
    }
    used = System.nanoTime() - start;
    System.out.println("unbatched time used: " + used);
    reporter.close();
    unbatched.update(DROP_TABLE);
  }

  static String randomString() {
    return String.valueOf(ThreadLocalRandom.current().nextLong());
  }
}
