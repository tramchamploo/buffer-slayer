package io.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.bufferslayer.DeferredHolder.newDeferred;
import static io.bufferslayer.MessageDroppedException.dropped;
import static io.bufferslayer.OverflowStrategy.Strategy.DropHead;
import static java.util.Collections.singletonList;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bufferslayer.Message.MessageKey;
import io.bufferslayer.OverflowStrategy.Strategy;
import io.bufferslayer.internal.Component;
import java.io.Flushable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
  private final ReporterMetrics metrics;

  private final long messageTimeoutNanos;
  private final int bufferedMaxMessages;

  private final boolean strictOrder;

  final QueueRecycler pendingRecycler;

  private final int flushThreadCount;
  final CopyOnWriteArraySet<Thread> flushThreads = new CopyOnWriteArraySet<>();

  private final AtomicBoolean closed = new AtomicBoolean(false);
  CountDownLatch close;

  @SuppressWarnings("unchecked")
  private AsyncReporter(Builder builder) {
    this.sender = new DefaultSenderToAsyncSenderAdaptor(builder.sender,
        builder.senderExecutor, builder.parallelismPerBatch);
    this.metrics = builder.metrics;
    this.messageTimeoutNanos = builder.messageTimeoutNanos;
    this.bufferedMaxMessages = builder.bufferedMaxMessages;
    this.strictOrder = builder.strictOrder;
    this.pendingRecycler = new RoundRobinQueueRecycler(builder.pendingMaxMessages,
        builder.overflowStrategy, builder.pendingKeepaliveNanos);
    this.flushThreadCount = builder.flushThreadCount;
    ThreadFactory flushThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("AsyncReporter-" + id + "-flush-thread-%d")
        .setDaemon(true)
        .build();
    if (messageTimeoutNanos > 0) {
      for (int i = 0; i < flushThreadCount; i++) {
        flushThreads.add(startFlushThread(flushThreadFactory));
      }
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
    int bufferedMaxMessages = 100;
    int pendingMaxMessages = 10000;
    int flushThreadCount = 5;
    long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
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
      checkArgument(parallelism >= 0, "parallelism >= 0: %s", parallelism);
      this.parallelismPerBatch = parallelism;
      return this;
    }

    public Builder metrics(ReporterMetrics metrics) {
      this.metrics = checkNotNull(metrics, "metrics");
      return this;
    }

    public Builder messageTimeout(long timeout, TimeUnit unit) {
      checkArgument(timeout >= 0, "timeout >= 0: %s", timeout);
      this.messageTimeoutNanos = unit.toNanos(timeout);
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

    public Builder flushThreadCount(int flushThreadCount) {
      checkArgument(flushThreadCount > 0, "flushThreadCount > 0: %s", flushThreadCount);
      this.flushThreadCount = flushThreadCount;
      return this;
    }

    public Builder pendingKeepalive(long keepalive, TimeUnit unit) {
      checkArgument(keepalive > 0, "keepalive > 0: %s", keepalive);
      this.pendingKeepaliveNanos = unit.toNanos(keepalive);
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

  private SizeBoundedQueue leaseQueue() {
    return pendingRecycler.lease(messageTimeoutNanos, TimeUnit.NANOSECONDS);
  }

  private void clearMetrics(Collection<MessageKey> keys) {
    for (MessageKey key: keys) {
      metrics.removeQueuedMessages(key);
    }
  }

  private Thread startFlushThread(final ThreadFactory threadFactory) {
    final BufferNextMessage consumer =
        new BufferNextMessage(bufferedMaxMessages, messageTimeoutNanos, strictOrder);
    Thread flushThread = threadFactory.newThread(new Runnable() {
      @Override
      public void run() {
        try {
          while (!closed.get()) {
            SizeBoundedQueue q = leaseQueue();
            if (q == null) continue;
            try {
              flush(q, consumer);
            } finally {
              pendingRecycler.recycle(q);
            }
          }
        } finally {
          // flush messages left in buffer
          List<Message> drained = consumer.drain();
          SizeBoundedQueue q;
          if (drained.size() > 0 && (q = leaseQueue()) != null) {
            for (Message message : drained) q.offer(message, newDeferred(message.id));
            flushTillEmpty(q);
          }
          // wake up notice thread
          if (closed.get()) close.countDown();
        }
      }
    });

    flushThread.start();
    return flushThread;
  }

  @Override
  public Promise<Object, MessageDroppedException, Integer> report(Message message) {
    checkNotNull(message, "message");
    metrics.incrementMessages(1);

    if (closed.get()) { // Drop the message when closed.
      DeferredObject<Object, MessageDroppedException, Integer> deferred = new DeferredObject<>();
      deferred.reject(dropped(new IllegalStateException("closed!"), singletonList(message)));
      return deferred.promise();
    }
    // If strictOrder is true, ignore original message key.
    Message.MessageKey key = message.asMessageKey();
    key = strictOrder ? Message.STRICT_ORDER : key;
    // Offer message to pending queue.
    SizeBoundedQueue pending = pendingRecycler.getOrCreate(key);
    Deferred<Object, MessageDroppedException, Integer> deferred = newDeferred(message.id);
    pending.offer(message, deferred);
    return deferred.promise().fail(metricsCallback());
  }

  /**
   * Update metrics if reporting failed.
   */
  private FailCallback<MessageDroppedException> metricsCallback() {
    return new FailCallback<MessageDroppedException>() {
      @Override
      public void onFail(MessageDroppedException ignored) {
        metrics.incrementMessagesDropped(1);
      }
    };
  }

  /**
   * Flush the queue right now.
   */
  private void flushTillEmpty(SizeBoundedQueue pending) {
    BufferNextMessage rightNow = new BufferNextMessage(bufferedMaxMessages, 0, strictOrder);
    while (pending.count > 0) flush(pending, rightNow);
  }

  @Override
  public void flush() {
    for (SizeBoundedQueue pending : pendingRecycler.elements()) {
      BufferNextMessage rightNow =
          new BufferNextMessage(bufferedMaxMessages, 0, strictOrder);
      flush(pending, rightNow);
    }
  }

  @SuppressWarnings("unchecked")
  void flush(SizeBoundedQueue pending, BufferNextMessage consumer) {
    int total = 0;
    do { // Drain the queue as many as we could
      int drained = pending.drainTo(consumer, consumer.remainingNanos());
      total += drained;
      if (drained == 0) break;
    } while (!consumer.isReady());
    // Remove overtime queues and relative metrics
    clearMetrics(pendingRecycler.shrink());

    if (total == 0) return;
    // Update metrics
    metrics.updateQueuedMessages(pending.key, pending.count);
    // Consumer should be ready to drain
    final List<Message> messages = consumer.drain();
    Promise<MultipleResults, OneReject, MasterProgress> promise = sender.send(messages);
    addCallbacks(messages, promise);
    try {
      promise.waitSafely();
    } catch (InterruptedException e) {
      logger.error("Interrupted flushing messages");
    }
  }

  /**
   * Resolve the promises returned to the caller by sending result.
   */
  private void addCallbacks(final List<Message> messages,
      Promise<MultipleResults, OneReject, MasterProgress> promise) {
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
  }

  /**
   * Log errors only when sending fails.
   */
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
    close = new CountDownLatch(messageTimeoutNanos > 0 ? flushThreadCount : 0);
    closed.set(true);
    try {
      if (!close.await(messageTimeoutNanos, TimeUnit.NANOSECONDS)) {
        logger.warn("Timed out waiting for close");
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupted waiting for close");
      Thread.currentThread().interrupt();
    }

    int count = 0;
    for (SizeBoundedQueue q : pendingRecycler.elements()) {
      count += q.clear();
      metrics.removeQueuedMessages(q.key);
    }
    if (count > 0) {
      metrics.incrementMessagesDropped(count);
      logger.warn("Dropped " + count + " messages due to AsyncReporter.close()");
    }
    pendingRecycler.clear();
  }
}
