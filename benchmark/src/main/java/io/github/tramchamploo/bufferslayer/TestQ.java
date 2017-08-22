package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.chmv8.LongAdderV8;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.jdeferred.impl.DeferredObject;

public class TestQ {

  public static void main(String[] args) {
    SizeBoundedQueue q = new SizeBoundedQueue(10000, OverflowStrategy.block, Message.SINGLE_KEY, MessageCounter.maxOf(50));

    LongAdderV8 offered = new LongAdderV8();
    LongAdderV8 drained = new LongAdderV8();

    TestMessage ONE = TestMessage.newMessage(0);
    for (int i = 0; i < 8; i++) {
      new Thread(() -> {
        while (true) {
          q.offer(ONE, new DeferredObject<>());
          offered.increment();
        }
      }).start();
    }

    new Thread(() -> {
      int i = 0;
      while (true) {
        q.drainTo(messages -> {
          drained.increment();
          return true;
        });
        i++;
        if (i == 10000) {
          q.clear();
          i = 0;
        }
      }
    }).start();

    Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
      System.out.println("Offered: " + offered.sum() + " Drained: " + drained.sum());
    }, 5, 5, TimeUnit.SECONDS);
  }
}
