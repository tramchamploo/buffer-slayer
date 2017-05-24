package io.bufferslayer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tramchamploo on 2017/5/23.
 * Schedule operations triggered by timer
 * Inspired by akka-stream
 */
abstract class TimeDriven<K> {

  class Timer {
    final long id;
    final Future task;

    Timer(long id, Future task) {
      this.id = id;
      this.task = task;
    }
  }

  private final Map<K, Timer> keyToTimer = new ConcurrentHashMap<>();
  private static AtomicLong timerIdGen = new AtomicLong();

  /**
   * Will be called when the scheduled timer is triggered.
   *
   * @param timerKey key of the scheduled timer
   */
  protected abstract void onTimer(K timerKey);

  /**
   * @return executor to schedule tasks
   */
  protected abstract ScheduledExecutorService scheduler();

  void schedulePeriodically(final K timerKey, long intervalNanos) {
    cancelTimer(timerKey);
    long id = timerIdGen.getAndIncrement();
    ScheduledFuture<?> task = scheduler().scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        onTimer(timerKey);
      }
    }, intervalNanos, intervalNanos, TimeUnit.NANOSECONDS);
    keyToTimer.put(timerKey, new Timer(id, task));
  }

  /**
   * Cancel timer, ensuring that the onTimer is not subsequently called.
   *
   * @param timerKey key of the timer to cancel
   */
  void cancelTimer(K timerKey) {
    Timer timer = keyToTimer.get(timerKey);
    if (timer != null) {
      timer.task.cancel(true);
      keyToTimer.remove(timerKey);
    }
  }

  boolean isTimerActive(K timerKey) {
    return keyToTimer.containsKey(timerKey);
  }

  void clearTimers() {
    for (Timer timer: keyToTimer.values()) {
      timer.task.cancel(true);
    }
    keyToTimer.clear();
  }
}
