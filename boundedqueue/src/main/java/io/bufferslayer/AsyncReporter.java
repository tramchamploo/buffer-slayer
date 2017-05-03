package io.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.bufferslayer.MessageDroppedException.dropped;
import static io.bufferslayer.OverflowStrategy.Strategy.DropHead;
import static java.util.Collections.singletonList;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bufferslayer.OverflowStrategy.Strategy;
import io.bufferslayer.internal.Component;
import java.io.Flushable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.jdeferred.Deferred;
import org.jdeferred.DoneCallback;
import org.jdeferred.FailCallback;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.jdeferred.multiple.MasterProgress;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneReject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by guohang.bao on 2017/2/28.
 */
public class AsyncReporter implements Reporter, Flushable, Component {

  private static final Logger logger = LoggerFactory.getLogger(AsyncReporter.class);

  static AtomicLong idGenerator = new AtomicLong();
  private final Long id = idGenerator.getAndIncrement();

  final AsyncSender sender;
  private final ReporterMetrics<SizeBoundedQueue> metrics;

  private final long messageTimeoutNanos;
  private final long flushThreadKeepaliveNanos;
  private final int bufferedMaxMessages;
  private final int pendingMaxMessages;
  private final boolean strictOrder;
  private final Strategy overflowStrategy;

  final Map<Message.MessageKey, SizeBoundedQueue> pendings;
  private final Lock pendingsLock;

  final AtomicBoolean closed = new AtomicBoolean(false);
  CountDownLatch close;

  final CopyOnWriteArraySet<Thread> flushThreads = new CopyOnWriteArraySet<>();
  private final ThreadFactory flushThreadFactory;
  private ScheduledExecutorService flushThreadsMonitor;

  @SuppressWarnings("unchecked")
  private AsyncReporter(Builder builder) {
    this.sender = new DefaultSenderToAsyncSenderAdaptor(builder.sender,
        builder.senderExecutor, builder.parallelismPerBatch);

    this.metrics = builder.metrics;

    this.messageTimeoutNanos = builder.messageTimeoutNanos;
    this.flushThreadKeepaliveNanos = builder.flushThreadKeepaliveNanos;
    this.bufferedMaxMessages = builder.bufferedMaxMessages;

    this.pendingMaxMessages = builder.pendingMaxMessages;
    this.pendings = new ConcurrentHashMap<>();
    this.pendingsLock = new ReentrantLock();

    this.strictOrder = builder.strictOrder;
    this.overflowStrategy = builder.overflowStrategy;

    flushThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("AsyncReporter-" + id + "-flush-thread-%d")
        .setDaemon(true)
        .build();

    if (!strictOrder && messageTimeoutNanos > 0) {
      flushThreadsMonitor = Executors.newSingleThreadScheduledExecutor();
      flushThreadsMonitor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          logger.info("AsyncReporter-{} flush thread count: {}", id, flushThreadCount());
        }
      }, 0, 5, TimeUnit.SECONDS);
    }
  }

  public static Builder builder(Sender sender) {
    return new Builder(sender);
  }

  public static final class Builder {
    final Sender sender;
    Executor senderExecutor = MoreExecutors.directExecutor();
    int parallelismPerBatch = 1;
    ReporterMetrics metrics = ReporterMetrics.NOOP_METRICS;
    long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
    long flushThreadKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
    int bufferedMaxMessages = 100;
    int pendingMaxMessages = 10000;
    boolean strictOrder = false;
    Strategy overflowStrategy = DropHead;

    Builder(Sender sender) {
      checkNotNull(sender, "sender");
      this.sender = sender;
    }

    public Builder senderExecutor(Executor executor) {
      this.senderExecutor = checkNotNull(executor, "executor");
      return this;
    }

    public Builder parallelismPerBatch(int parallelism) {
      checkArgument(parallelism >= 0, "parallelism < 1: %s", parallelism);
      this.parallelismPerBatch = parallelism;
      return this;
    }

    public Builder metrics(ReporterMetrics metrics) {
      this.metrics = checkNotNull(metrics, "metrics");
      return this;
    }

    public Builder messageTimeout(long timeout, TimeUnit unit) {
      checkArgument(timeout >= 0, "timeout < 0: %s", timeout);
      this.messageTimeoutNanos = unit.toNanos(timeout);
      return this;
    }

    public Builder flushThreadKeepalive(long keepalive, TimeUnit unit) {
      checkArgument(keepalive > 0, "keepalive > 0: %s", keepalive);
      this.flushThreadKeepaliveNanos = unit.toNanos(keepalive);
      return this;
    }

    public Builder bufferedMaxMessages(int bufferedMaxMessages) {
      checkArgument(bufferedMaxMessages > 0, "bufferedMaxMessages > 0: %s", bufferedMaxMessages);
      this.bufferedMaxMessages = bufferedMaxMessages;
      return this;
    }

    public Builder pendingMaxMessages(int pendingMaxMessages) {
      checkArgument(pendingMaxMessages > 0, "pendingMaxMessages > 0: %s", pendingMaxMessages);
      this.pendingMaxMessages = pendingMaxMessages;
      return this;
    }

    public Builder strictOrder(boolean strictOrder) {
      this.strictOrder = strictOrder;
      return this;
    }

    public Builder overflowStrategy(Strategy overflowStrategy) {
      this.overflowStrategy = overflowStrategy;
      return this;
    }

    public AsyncReporter build() {
      return new AsyncReporter(this);
    }
  }

  int flushThreadCount() {
    int c = 0;
    for (Thread flushThread : flushThreads) {
      if (flushThread.isAlive()) c++;
      else flushThreads.remove(flushThread);
    }
    return c;
  }

  private SizeBoundedQueue getOrInitializePendingQueue(Message.MessageKey key) {
    key = strictOrder ? Message.STRICT_ORDER : key;
    SizeBoundedQueue pending = pendings.get(key);
    if (pending == null) {
      // race condition initializing pending queue
      try {
        pendingsLock.lock();
        pending = pendings.get(key);
        if (pending == null) {
          pending = new SizeBoundedQueue(pendingMaxMessages, overflowStrategy);
          pendings.put(key, pending);
          if (messageTimeoutNanos > 0) {
            flushThreads.add(startFlushThread(key, pending));
          }
        }
      } finally {
        pendingsLock.unlock();
      }
    }
    return pending;
  }

  @Override
  public Promise<Object, MessageDroppedException, Integer> report(Message message) {
    checkNotNull(message, "message");
    metrics.incrementMessages(1);

    if (closed.get()) {
      DeferredObject<Object, MessageDroppedException, Integer> deferred = new DeferredObject<>();
      deferred.reject(dropped(new IllegalStateException("closed!"), singletonList(message)));
      return deferred.promise();
    }

    Message.MessageKey key = message.asMessageKey();
    SizeBoundedQueue pending = getOrInitializePendingQueue(key);

    Deferred<Object, MessageDroppedException, Integer> deferred =
        DeferredHolder.newDeferred(message.id);
    pending.offer(message, deferred);
    return deferred.promise().fail(metricsCallback());
  }

  private FailCallback<MessageDroppedException> metricsCallback() {
    return new FailCallback<MessageDroppedException>() {
      @Override
      public void onFail(MessageDroppedException ignored) {
        metrics.incrementMessagesDropped(1);
      }
    };
  }

  private Thread startFlushThread(final Message.MessageKey key, final SizeBoundedQueue pending) {
    final BufferNextMessage consumer =
        new BufferNextMessage(bufferedMaxMessages, messageTimeoutNanos, strictOrder);
    final Thread flushThread = flushThreadFactory.newThread(new Runnable() {
      @Override
      public void run() {
        long lastDrained = System.nanoTime();
        try {
          while (!closed.get()) {
            if (!strictOrder && // check if exceeds keepAlive
                flushThreadKeepaliveNanos > 0 &&
                System.nanoTime() - lastDrained >= flushThreadKeepaliveNanos) {
              return;
            }
            int drainedCount = flush(pending, consumer);
            if (!strictOrder && drainedCount > 0) {
              lastDrained = System.nanoTime();
            }
          }
        } finally {
          for (Message message : consumer.drain()) { // flush messages left in queue
            pending.offer(message, DeferredHolder.newDeferred(message.id));
          }
          flushTillEmpty(pending);

          pendings.remove(key); // remove queue from pendings
          metrics.removeQueuedMessages(pending); // remove metrics
          if (closed.get()) close.countDown(); // wake up notice thread
        }
      }
    });

    flushThread.start();
    return flushThread;
  }

  void flushTillEmpty(SizeBoundedQueue pending) {
    BufferNextMessage rightNow =
        new BufferNextMessage(bufferedMaxMessages, 0, strictOrder);
    while (pending.count > 0) flush(pending, rightNow);
  }

  @Override
  public void flush() {
    for (SizeBoundedQueue pending : pendings.values()) {
      BufferNextMessage rightNow =
          new BufferNextMessage(bufferedMaxMessages, 0, strictOrder);
      flush(pending, rightNow);
    }
  }

  @SuppressWarnings("unchecked")
  int flush(SizeBoundedQueue pending, BufferNextMessage consumer) {
    int drainedCount = pending.drainTo(consumer, consumer.remainingNanos());
    metrics.updateQueuedMessages(pending, pending.count);

    if (!consumer.isReady()) {
      return drainedCount;
    }

    final List<Message> messages = consumer.drain();
    Promise<MultipleResults, OneReject, MasterProgress> promise = sender.send(messages);

    promise.done(new DoneCallback<MultipleResults>() {
      @Override
      public void onDone(MultipleResults result) {
        DeferredHolder.batchResolve(messages, DeferredUtil.toResults(result));
      }
    }).fail(new FailCallback<OneReject>() {
      @Override
      public void onFail(OneReject reject) {
        DeferredHolder.batchReject(messages, (MessageDroppedException) reject.getReject());
      }
    }).fail(loggingCallback());
    try {
      promise.waitSafely();
      if (promise.isResolved()) return drainedCount;
    } catch (InterruptedException e) {
      logger.error("Interrupted flushing messages");
      Thread.currentThread().interrupt();
    }
    return 0;
  }

  private FailCallback<OneReject> loggingCallback() {
    return new FailCallback<OneReject>() {
      @Override
      public void onFail(OneReject reject) {
        MessageDroppedException ex = (MessageDroppedException) reject.getReject();
        logger.warn(ex.getMessage());
      }
    };
  }

  @Override
  public CheckResult check() {
    return sender.check();
  }

  @Override
  public void close() {
    close = new CountDownLatch(messageTimeoutNanos > 0 ? flushThreads.size() : 0);
    closed.set(true);
    try {
      if (!close.await(messageTimeoutNanos, TimeUnit.NANOSECONDS)) {
        logger.warn("Timed out waiting for close");
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupted waiting for close");
      Thread.currentThread().interrupt();
    }

    if (flushThreadsMonitor != null) flushThreadsMonitor.shutdown();

    int count = 0;
    for (SizeBoundedQueue pending : pendings.values()) {
      count += pending.clear();
    }
    if (count > 0) {
      metrics.incrementMessagesDropped(count);
      logger.warn("Dropped " + count + " messages due to AsyncReporter.close()");
    }
  }
}
