package io.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.bufferslayer.DeferredHolder.newDeferred;
import static io.bufferslayer.MessageDroppedException.dropped;
import static io.bufferslayer.OverflowStrategy.Strategy.DropHead;
import static java.util.Collections.singletonList;

import com.google.common.base.Strings;
import io.bufferslayer.Message.MessageKey;
import io.bufferslayer.OverflowStrategy.Strategy;
import io.bufferslayer.internal.Component;
import java.io.Flushable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
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
public class AsyncReporter implements Reporter, Flushable, Component {

  private static final Logger logger = LoggerFactory.getLogger(AsyncReporter.class);
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
  final QueueRecycler pendingRecycler;
  final AtomicBoolean started = new AtomicBoolean(false);
  final AtomicBoolean closed = new AtomicBoolean(false);
  CountDownLatch close;

  @SuppressWarnings("unchecked")
  private AsyncReporter(Builder builder) {
    this.sender = new SenderToAsyncSenderAdaptor(builder.sender, id, builder.senderThreads);
    this.metrics = builder.metrics;
    this.messageTimeoutNanos = builder.messageTimeoutNanos;
    this.bufferedMaxMessages = builder.bufferedMaxMessages;
    this.strictOrder = builder.strictOrder;
    this.flushThreads = builder.flushThreads;
    this.flushThreadFactory = new FlushThreadFactory(this);
    switch (builder.recycler.toLowerCase()) {
      case "roundrobin":
        this.pendingRecycler = new RoundRobinQueueRecycler(builder.pendingMaxMessages,
            builder.overflowStrategy, builder.pendingKeepaliveNanos); break;
      default:
        this.pendingRecycler = new SizeFirstQueueRecycler(builder.pendingMaxMessages,
            builder.overflowStrategy, builder.pendingKeepaliveNanos);
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
    int flushThreads = 5;
    long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
    boolean strictOrder = false;
    Strategy overflowStrategy = DropHead;
    String recycler = "sizefirst";

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

    public Builder recycler(String recycler) {
      checkArgument(!Strings.isNullOrEmpty(recycler), "recycler must not be empty");
      this.recycler = recycler;
      return this;
    }

    public AsyncReporter build() {
      return new AsyncReporter(this);
    }
  }

  private void clearMetrics(Collection<MessageKey> keys) {
    for (MessageKey key: keys) {
      metrics.removeQueuedMessages(key);
    }
  }

  private void startFlushThreads() {
    for (int i = 0; messageTimeoutNanos > 0 && i < flushThreads; i++) {
      Thread thread = flushThreadFactory.newFlushThread();
      thread.start();
      flushers.add(thread);
    }
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
    if (started.compareAndSet(false, true)) {
      startFlushThreads();
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
  void flushTillEmpty(SizeBoundedQueue pending) {
    BufferNextMessage rightNow = new BufferNextMessage(bufferedMaxMessages, 0, strictOrder);
    while (pending.count > 0) blockingFlush(pending, rightNow);
  }

  @Override
  public void flush() {
    for (SizeBoundedQueue pending : pendingRecycler.elements()) {
      BufferNextMessage rightNow =
          new BufferNextMessage(bufferedMaxMessages, 0, strictOrder);
      blockingFlush(pending, rightNow);
    }
  }

  private void blockingFlush(SizeBoundedQueue pending, BufferNextMessage buffer) {
    Promise<List<?>, MessageDroppedException, ?> promise = flush(pending, buffer);
    try {
      promise.waitSafely();
    } catch (InterruptedException e) {
      logger.warn("Interrupted flushing messages.");
    }
  }

  @SuppressWarnings("unchecked")
  Promise<List<?>, MessageDroppedException, ?> flush(SizeBoundedQueue pending,
      BufferNextMessage consumer) {
    int total = 0;
    do { // Drain the queue as many as we could
      int drained = pending.drainTo(consumer, consumer.remainingNanos());
      total += drained;
      if (drained == 0) break;
    } while (!consumer.isReady());
    // Remove overtime queues and relative metrics
    clearMetrics(pendingRecycler.shrink());

    if (total == 0) {
      DeferredObject<List<?>, MessageDroppedException, ?> empty = new DeferredObject<>();
      empty.resolve(null);
      return empty.promise();
    }
    // Update metrics
    metrics.updateQueuedMessages(pending.key, pending.count);
    // Consumer should be ready to drain
    final List<Message> messages = consumer.drain();
    Promise<List<?>, MessageDroppedException, ?> promise = sender.send(messages);
    addCallbacks(messages, promise);
    return promise;
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
    close = new CountDownLatch(messageTimeoutNanos > 0 ? flushThreads : 0);
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
