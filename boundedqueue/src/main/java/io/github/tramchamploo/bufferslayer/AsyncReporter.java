package io.github.tramchamploo.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.github.tramchamploo.bufferslayer.MessageDroppedException.dropped;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import io.github.tramchamploo.bufferslayer.QueueManager.Callback;
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
public class AsyncReporter<M extends Message, R> extends TimeDriven<MessageKey> implements Reporter<M, R>, Flushable {

  static final Logger logger = LoggerFactory.getLogger(AsyncReporter.class);
  static final int DEFAULT_TIMER_THREADS = Runtime.getRuntime().availableProcessors();
  static final int MAX_BUFFER_POOL_ENTRIES = 1000;

  static AtomicLong idGenerator = new AtomicLong();
  final Long id = idGenerator.getAndIncrement();
  final AsyncSender<M, R> sender;
  final ReporterMetrics metrics;
  final long messageTimeoutNanos;
  final int bufferedMaxMessages;
  final boolean singleKey;
  final int flushThreads;
  final FlushThreadFactory flushThreadFactory;
  final CopyOnWriteArraySet<Thread> flushers = new CopyOnWriteArraySet<>();
  final FlushSynchronizer synchronizer = new FlushSynchronizer();
  final QueueManager queueManager;
  final AtomicBoolean started = new AtomicBoolean(false);
  final AtomicBoolean closed = new AtomicBoolean(false);
  CountDownLatch close;
  ScheduledExecutorService scheduler;
  BufferPool bufferPool;

  private AsyncReporter(Builder<M, R> builder) {
    this.sender = new SenderToAsyncSenderAdaptor<>(builder.sender, id, builder.senderThreads);
    this.metrics = builder.metrics;
    this.messageTimeoutNanos = builder.messageTimeoutNanos;
    this.bufferedMaxMessages = builder.bufferedMaxMessages;
    this.singleKey = builder.singleKey;
    this.flushThreads = builder.flushThreads;
    this.queueManager = new QueueManager(builder.pendingMaxMessages,
                                         builder.overflowStrategy,
                                         builder.pendingKeepaliveNanos);
    this.flushThreadFactory = new FlushThreadFactory(this);
    this.bufferPool = new BufferPool(MAX_BUFFER_POOL_ENTRIES, builder.bufferedMaxMessages, builder.singleKey);
    if (messageTimeoutNanos > 0) {
      ThreadFactory timerFactory = new ThreadFactoryBuilder()
                                      .setNameFormat("AsyncReporter-" + id + "-timer-%d")
                                      .setDaemon(true)
                                      .build();
      ScheduledThreadPoolExecutor timerPool = new ScheduledThreadPoolExecutor(builder.timerThreads, timerFactory);
      timerPool.setRemoveOnCancelPolicy(true);
      this.scheduler = timerPool;
      this.queueManager.onCreate(new Callback() {
        @Override
        public void call(SizeBoundedQueue queue) {
          schedulePeriodically(queue.key, messageTimeoutNanos);
        }
      });
    }
  }

  public static <M extends Message, R> Builder<M, R> builder(Sender<M, R> sender) {
    return new Builder<>(sender);
  }

  public static final class Builder<M extends Message, R> extends Reporter.Builder<Builder<M, R>, M, R> {

    int senderThreads = 1;
    int flushThreads = 1;
    int timerThreads = DEFAULT_TIMER_THREADS;
    long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
    boolean singleKey = false;

    Builder(Sender<M, R> sender) {
      super(sender);
    }

    public Builder<M, R> senderThreads(int senderThreads) {
      checkArgument(senderThreads > 0, "senderThreads > 0: %s", senderThreads);
      this.senderThreads = senderThreads;
      return this;
    }

    public Builder<M, R> flushThreads(int flushThreads) {
      checkArgument(flushThreads > 0, "flushThreads > 0: %s", flushThreads);
      this.flushThreads = flushThreads;
      return this;
    }

    public Builder<M, R> timerThreads(int timerThreads) {
      checkArgument(timerThreads > 0, "timerThreads > 0: %s", timerThreads);
      this.timerThreads = timerThreads;
      return this;
    }

    public Builder<M, R> pendingKeepalive(long keepalive, TimeUnit unit) {
      checkArgument(keepalive > 0, "keepalive > 0: %s", keepalive);
      this.pendingKeepaliveNanos = unit.toNanos(keepalive);
      return this;
    }

    public Builder<M, R> singleKey(boolean singleKey) {
      this.singleKey = singleKey;
      return this;
    }

    public AsyncReporter<M, R> build() {
      return new AsyncReporter<>(this);
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
  public Promise<R, MessageDroppedException, Integer> report(M message) {
    checkNotNull(message, "message");
    metrics.incrementMessages(1);

    if (closed.get()) { // Drop the message when closed.
      DeferredObject<R, MessageDroppedException, Integer> deferred = new DeferredObject<>();
      deferred.reject(dropped(new IllegalStateException("closed!"), singletonList(message)));
      return deferred.promise().fail(metricsCallback());
    }

    // Lazy initialize flush threads
    if (messageTimeoutNanos > 0 &&
        started.compareAndSet(false, true)) {
      startFlushThreads();
    }
    // If singleKey is true, ignore original message key.
    Message.MessageKey key = message.asMessageKey();
    key = singleKey ? Message.SINGLE_KEY : key;

    // Offer message to pending queue.
    SizeBoundedQueue pending = queueManager.getOrCreate(key);
    Deferred<R, MessageDroppedException, Integer> deferred = DeferredHolder.newDeferred(message.id);
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
        Promise<List<R>, MessageDroppedException, ?> promise = flush(pending);
        promise.waitSafely();
      }
    } catch (InterruptedException e) {
      logger.warn("Interrupted flushing messages.");
    }
  }

  @SuppressWarnings("unchecked")
  Promise<List<R>, MessageDroppedException, ?> flush(SizeBoundedQueue pending) {
    Buffer buffer = bufferPool.acquire();
    try {
      int drained = pending.drainTo(buffer);
      // Remove overtime queues and relative metrics and timer
      List<MessageKey> shrinked = queueManager.shrink();
      clearMetricsAndTimer(shrinked);
      if (drained == 0) {
        List<R> empty = emptyList();
        return new DeferredObject<List<R>, MessageDroppedException, Object>()
            .resolve(empty)
            .promise();
      }
      // Update metrics
      metrics.updateQueuedMessages(pending.key, pending.count);
      final List<M> messages = (List<M>) buffer.drain();
      Promise<List<R>, MessageDroppedException, ?> promise = sender.send(messages);
      addCallbacks(messages, promise);
      return promise;
    } finally {
      bufferPool.release(buffer);
    }
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
  private void addCallbacks(final List<M> messages,
      Promise<List<R>, MessageDroppedException, ?> promise) {
    promise.done(new DoneCallback<List<R>>() {
      @Override
      public void onDone(List<R> result) {
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
