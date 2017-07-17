package io.github.tramchamploo.bufferslayer;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.github.tramchamploo.bufferslayer.DeferredHolder.newDeferred;
import static io.github.tramchamploo.bufferslayer.MessageDroppedException.dropped;
import static java.util.Collections.singletonList;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import io.reactivex.BackpressureOverflowStrategy;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.Scheduler;
import io.reactivex.flowables.GroupedFlowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jdeferred.Deferred;
import org.jdeferred.FailCallback;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RxReporter<M extends Message, R> implements Reporter<M, R>, FlowableOnSubscribe<M> {

  static final Logger logger = LoggerFactory.getLogger(RxReporter.class);

  final long messageTimeoutNanos;
  final int bufferedMaxMessages;
  final int pendingMaxMessages;
  final Strategy overflowStrategy;
  final Sender<M, R> sender;
  final ReporterMetrics metrics;
  final AtomicBoolean closed = new AtomicBoolean(false);

  private FlowableEmitter<M> emitter;
  private Scheduler scheduler;

  private RxReporter(Builder<M, R> builder) {
    this.sender = builder.sender;
    this.metrics = builder.metrics;

    this.messageTimeoutNanos = builder.messageTimeoutNanos;
    this.bufferedMaxMessages = builder.bufferedMaxMessages;
    this.pendingMaxMessages = builder.pendingMaxMessages;
    this.overflowStrategy = builder.overflowStrategy;
    this.scheduler = builder.scheduler;

    Flowable<M> flowable = Flowable.create(this, BackpressureStrategy.MISSING);
    initBackpressurePolicy(flowable)
        .observeOn(Schedulers.single())
        .groupBy(new MessagePartitioner<>())
        .subscribe(new MessageGroupSubscriber<>(messageTimeoutNanos, bufferedMaxMessages, sender, scheduler));
  }

  public static <M extends Message, R> Builder<M, R> builder(Sender<M, R> sender) {
    return new Builder<>(sender);
  }

  private Flowable<M> initBackpressurePolicy(Flowable<M> flowable) {
    Strategy strategy = this.overflowStrategy;
    if (strategy == Strategy.DropNew) {
      return flowable.onBackpressureDrop(new Consumer<M>() {
        @Override
        public void accept(M m) throws Exception {
          metricsCallback(1);
        }
      });
    } else {
      BackpressureOverflowStrategy rxStrategy = RxOverflowStrategyBridge.toRxStrategy(strategy);
      return flowable.onBackpressureBuffer(pendingMaxMessages, new Action() {
        @Override
        public void run() throws Exception {
          metricsCallback(1);
        }
      }, rxStrategy);
    }
  }

  private void metricsCallback(int quantity) {
    metrics.incrementMessagesDropped(quantity);
  }

  public static final class Builder<M extends Message, R> extends Reporter.Builder<Builder<M, R>, M, R> {

    Scheduler scheduler = Schedulers.io();

    Builder(Sender<M, R> sender) {
      super(sender);
    }

    public Builder<M, R> scheduler(Scheduler scheduler) {
      this.scheduler = checkNotNull(scheduler, "scheduler");
      return this;
    }

    @Override
    public Reporter<M, R> build() {
      return new RxReporter<>(this);
    }
  }

  private static class MessageGroupSubscriber<M extends Message, R>
      implements Consumer<GroupedFlowable<MessageKey, M>> {

    final long messageTimeoutNanos;
    final int bufferedMaxMessages;
    final Sender<M, R> sender;
    final Scheduler scheduler;

    private MessageGroupSubscriber(long messageTimeoutNanos,
                                   int bufferedMaxMessages,
                                   Sender<M, R> sender,
                                   Scheduler scheduler) {
      this.messageTimeoutNanos = messageTimeoutNanos == 0 ? Long.MAX_VALUE : messageTimeoutNanos;
      this.bufferedMaxMessages = bufferedMaxMessages;
      this.sender = sender;
      this.scheduler = scheduler;
    }

    @Override
    public void accept(GroupedFlowable<MessageKey, M> group) throws Exception {
      Flowable<List<M>> bufferedMessages = group.buffer(messageTimeoutNanos, TimeUnit.NANOSECONDS, scheduler, bufferedMaxMessages);
      bufferedMessages.subscribeOn(scheduler).subscribe(SenderConsumerBridge.toConsumer(sender));
    }
  }

  private static class MessagePartitioner<M extends Message> implements Function<M, MessageKey> {

    @Override
    public MessageKey apply(M m) throws Exception {
      return m.asMessageKey();
    }
  }

  @Override
  public CheckResult check() {
    return sender.check();
  }

  @Override
  public Promise<R, MessageDroppedException, ?> report(M message) {
    checkNotNull(message, "message");
    metrics.incrementMessages(1);

    if (closed.get()) { // Drop the message when closed.
      DeferredObject<R, MessageDroppedException, Integer> deferred = new DeferredObject<>();
      deferred.reject(dropped(new IllegalStateException("closed!"), singletonList(message)));
      metricsCallback(1);
      return deferred.promise();
    }

    Deferred<R, MessageDroppedException, Integer> deferred = newDeferred(message.id);
    emitter.onNext(message);
    return deferred.promise().fail(loggingCallback());
  }

  private FailCallback<MessageDroppedException> loggingCallback() {
    return new FailCallback<MessageDroppedException>() {
      @Override
      public void onFail(MessageDroppedException reject) {
        metricsCallback(reject.dropped.size());
        logger.warn(reject.getMessage());
      }
    };
  }

  @Override
  public void subscribe(FlowableEmitter<M> emitter) throws Exception {
    this.emitter = emitter;
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) return;
    emitter.onComplete();
  }
}
