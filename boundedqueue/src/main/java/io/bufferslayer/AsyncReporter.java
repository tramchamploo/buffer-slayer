package io.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.bufferslayer.DeferredHolder.newDeferred;
import static io.bufferslayer.MessageDroppedException.dropped;
import static io.bufferslayer.OverflowStrategy.Strategy.DropHead;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bufferslayer.Message.MessageKey;
import io.bufferslayer.OverflowStrategy.Strategy;
import io.bufferslayer.QueueManager.Callback;
import io.bufferslayer.internal.Component;
import java.io.Flushable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jdeferred.Deferred;
import org.jdeferred.DoneCallback;
import org.jdeferred.FailCallback;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tramchamploo on 2017/2/28.
 */
public class AsyncReporter extends TimeDriven<MessageKey> implements Reporter, Flushable, Component {

  static final Logger logger = LoggerFactory.getLogger(AsyncReporter.class);
  static final int DEFAULT_TIMER_THREADS = Runtime.getRuntime().availableProcessors();

  static AtomicLong idGenerator = new AtomicLong();
  final Long id = idGenerator.getAndIncrement();
  final AsyncSender sender;
  final ReporterMetrics metrics;
  final long messageTimeoutNanos;
  final int bufferedMaxMessages;
  final boolean strictOrder;
  final int flushThreads;
  final FlushThreadFactory flushThreadFactory;
  final CopyOnWriteArraySet<Thread> flushers = new CopyOnWriteArraySet<>();
  final FlushSynchronizer synchronizer = new FlushSynchronizer();
  final QueueManager queueManager;
  final AtomicBoolean started = new AtomicBoolean(false);
  final AtomicBoolean closed = new AtomicBoolean(false);
  CountDownLatch close;
  ScheduledExecutorService scheduler;

  @SuppressWarnings("unchecked")
  private AsyncReporter(Builder builder) {
    this.sender = new SenderToAsyncSenderAdaptor(builder.sender, id, builder.senderThreads);
    this.metrics = builder.metrics;
    this.messageTimeoutNanos = builder.messageTimeoutNanos;
    this.bufferedMaxMessages = builder.bufferedMaxMessages;
    this.strictOrder = builder.strictOrder;
    this.flushThreads = builder.flushThreads;
    this.queueManager = new QueueManager(builder.pendingMaxMessages,
        builder.overflowStrategy, builder.pendingKeepaliveNanos);
    this.flushThreadFactory = new FlushThreadFactory(this);
    if (messageTimeoutNanos > 0) {
      ThreadFactory timerFactory = new ThreadFactoryBuilder()
                                      .setNameFormat("AsyncReporter-" + id + "-timer-%d")
                                      .setDaemon(true)
                                      .build();
      ScheduledThreadPoolExecutor timerPool =
          new ScheduledThreadPoolExecutor(builder.timerThreads, timerFactory);
      timerPool.setRemoveOnCancelPolicy(true);
      this.scheduler = timerPool;
      this.queueManager.createCallback(new Callback() {
        @Override
        public void call(SizeBoundedQueue queue) {
          schedulePeriodically(queue.key, messageTimeoutNanos);
        }
      });
    }
  }

  public static Builder builder(Sender sender) {
    return new Builder(sender);
  }

  public static final class Builder {

    final Sender sender;
    int senderThreads = 1;
    ReporterMetrics metrics = ReporterMetrics.NOOP_METRICS;
    long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
    int bufferedMaxMessages = 100;
    int pendingMaxMessages = 10000;
    int flushThreads = 1;
    int timerThreads = DEFAULT_TIMER_THREADS;
    long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
    boolean strictOrder = false;
    Strategy overflowStrategy = DropHead;

    Builder(Sender sender) {
      checkNotNull(sender, "sender");
      this.sender = sender;
    }

    public Builder senderThreads(int senderThreads) {
      checkArgument(senderThreads > 0, "senderThreads > 0: %s", senderThreads);
      this.senderThreads = senderThreads;
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

    public Builder flushThreads(int flushThreads) {
      checkArgument(flushThreads > 0, "flushThreads > 0: %s", flushThreads);
      this.flushThreads = flushThreads;
      return this;
    }

    public Builder timerThreads(int timerThreads) {
      checkArgument(timerThreads > 0, "timerThreads > 0: %s", timerThreads);
      this.timerThreads = timerThreads;
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

  @Override
  protected void onTimer(MessageKey timerKey) {
    SizeBoundedQueue q = queueManager.get(timerKey);
    if (q != null && q.size() > 0) flush(q);
  }

  @Override
  protected ScheduledExecutorService scheduler() {
    return scheduler;
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

    // Lazy initialize flush threads
    if (messageTimeoutNanos > 0 &&
        started.compareAndSet(false, true)) {
      startFlushThreads();
    }
    // If strictOrder is true, ignore original message key.
    Message.MessageKey key = message.asMessageKey();
    key = strictOrder ? Message.STRICT_ORDER : key;

    // Offer message to pending queue.
    SizeBoundedQueue pending = queueManager.getOrCreate(key);
    Deferred<Object, MessageDroppedException, Integer> deferred = newDeferred(message.id);
    pending.offer(message, deferred);
    if (pending.size() >= bufferedMaxMessages)
      synchronizer.offer(pending);
    return deferred.promise().fail(metricsCallback());
  }

  private void startFlushThreads() {
    for (int i = 0; i < flushThreads; i++) {
      Thread thread = flushThreadFactory.newFlushThread();
      thread.start();
      flushers.add(thread);
    }
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

  @Override
  public void flush() {
    try {
      for (SizeBoundedQueue pending : queueManager.elements()) {
        Promise<List<?>, MessageDroppedException, ?> promise = flush(pending);
        promise.waitSafely();
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupted flushing messages.");
    }
  }

  @SuppressWarnings("unchecked")
  Promise<List<?>, MessageDroppedException, ?> flush(SizeBoundedQueue pending) {
    BufferNextMessage buffer = new BufferNextMessage(bufferedMaxMessages, strictOrder);
    int drained = pending.drainTo(buffer);
    // Remove overtime queues and relative metrics and timer
    List<MessageKey> shrinked = queueManager.shrink();
    clearMetricsAndTimer(shrinked);
    if (drained == 0) {
      return new DeferredObject<List<?>, MessageDroppedException, Object>()
          .resolve(emptyList())
          .promise();
    }
    // Update metrics
    metrics.updateQueuedMessages(pending.key, pending.count);
    final List<Message> messages = buffer.drain();
    Promise<List<?>, MessageDroppedException, ?> promise = sender.send(messages);
    addCallbacks(messages, promise);
    return promise;
  }

  private void clearMetricsAndTimer(Collection<MessageKey> keys) {
    for (MessageKey key: keys) {
      metrics.removeQueuedMessages(key);
      if (isTimerActive(key)) cancelTimer(key);
    }
  }

  /**
   * Resolve the promises returned to the caller by sending result.
   */
  private void addCallbacks(final List<Message> messages,
                            Promise<List<?>, MessageDroppedException, ?> promise) {
    promise.done(new DoneCallback<List<?>>() {
      @Override
      public void onDone(List<?> result) {
        DeferredHolder.batchResolve(messages, result);
      }
    }).fail(new FailCallback<MessageDroppedException>() {
      @Override
      public void onFail(MessageDroppedException reject) {
        DeferredHolder.batchReject(messages, reject);
      }
    }).fail(loggingCallback());
  }

  /**
   * Log errors only when sending fails.
   */
  private FailCallback<MessageDroppedException> loggingCallback() {
    return new FailCallback<MessageDroppedException>() {
      @Override
      public void onFail(MessageDroppedException reject) {
        logger.warn(reject.getMessage());
      }
    };
  }

  @Override
  public CheckResult check() {
    return sender.check();
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) return;
    close = new CountDownLatch(messageTimeoutNanos > 0 ? flushThreads : 0);
    try {
      if (!close.await(messageTimeoutNanos, TimeUnit.NANOSECONDS)) {
        logger.warn("Timed out waiting for close");
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupted waiting for close");
      Thread.currentThread().interrupt();
    }
    flush();
    clearPendings();
    if (scheduler != null) {
      clearTimers();
      scheduler.shutdown();
    }
  }

  private void clearPendings() {
    int count = 0;
    for (SizeBoundedQueue q : queueManager.elements()) {
      count += q.clear();
      metrics.removeQueuedMessages(q.key);
    }
    queueManager.clear();
    if (count > 0) {
      metrics.incrementMessagesDropped(count);
      logger.warn("Dropped " + count + " messages due to AsyncReporter.close()");
    }
  }
}
