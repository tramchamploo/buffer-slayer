package io.github.tramchamploo.bufferslayer;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.github.tramchamploo.bufferslayer.MessageDroppedException.dropped;
import static java.util.Collections.singletonList;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import io.github.tramchamploo.bufferslayer.internal.Future;
import io.github.tramchamploo.bufferslayer.internal.FutureListener;
import io.github.tramchamploo.bufferslayer.internal.MessageFuture;
import io.github.tramchamploo.bufferslayer.internal.MessagePromise;
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RxReporter<M extends Message, R> implements Reporter<M, R>, FlowableOnSubscribe<MessagePromise<R>> {

  static final Logger logger = LoggerFactory.getLogger(RxReporter.class);

  final long messageTimeoutNanos;
  final int bufferedMaxMessages;
  final int pendingMaxMessages;
  final Strategy overflowStrategy;
  final Sender<M, R> sender;
  final ReporterMetrics metrics;
  final AtomicBoolean closed = new AtomicBoolean(false);

  private FlowableEmitter<MessagePromise<R>> emitter;
  private Scheduler scheduler;

  private RxReporter(Builder<M, R> builder) {
    this.sender = builder.sender;
    this.metrics = builder.metrics;

    this.messageTimeoutNanos = builder.messageTimeoutNanos;
    this.bufferedMaxMessages = builder.bufferedMaxMessages;
    this.pendingMaxMessages = builder.pendingMaxMessages;
    this.overflowStrategy = builder.overflowStrategy;
    this.scheduler = builder.scheduler;

    Flowable<MessagePromise<R>> flowable = Flowable.create(this, BackpressureStrategy.MISSING);
    initBackpressurePolicy(flowable)
        .observeOn(Schedulers.single())
        .groupBy(new MessagePartitioner())
        .subscribe(new MessageGroupSubscriber(messageTimeoutNanos, bufferedMaxMessages, sender, scheduler));
  }

  public static <M extends Message, R> Builder<M, R> builder(Sender<M, R> sender) {
    return new Builder<>(sender);
  }

  private Flowable<MessagePromise<R>> initBackpressurePolicy(Flowable<MessagePromise<R>> flowable) {
    Strategy strategy = this.overflowStrategy;
    if (strategy == Strategy.DropNew) {
      return flowable.onBackpressureDrop(new Consumer<MessagePromise<R>>() {
        @Override
        public void accept(MessagePromise<R> promise) {
          metricsCallback(1);
        }
      });
    } else {
      BackpressureOverflowStrategy rxStrategy = RxOverflowStrategyBridge.toRxStrategy(strategy);
      return flowable.onBackpressureBuffer(pendingMaxMessages, new Action() {
        @Override
        public void run() {
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
    public RxReporter<M, R> build() {
      return new RxReporter<>(this);
    }
  }

  private class MessageGroupSubscriber implements Consumer<GroupedFlowable<MessageKey, MessagePromise<R>>> {

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
    public void accept(GroupedFlowable<MessageKey, MessagePromise<R>> group) {
      Flowable<List<MessagePromise<R>>> bufferedMessages =
          group.buffer(messageTimeoutNanos, TimeUnit.NANOSECONDS, scheduler, bufferedMaxMessages);
      bufferedMessages.subscribeOn(scheduler).subscribe(SenderConsumerBridge.toConsumer(sender));
    }
  }

  private class MessagePartitioner implements Function<MessagePromise<R>, MessageKey> {

    @Override
    public MessageKey apply(MessagePromise<R> promise) {
      return promise.message().asMessageKey();
    }
  }

  @Override
  public CheckResult check() {
    return sender.check();
  }

  @Override
  @SuppressWarnings("unchecked")
  public MessageFuture<R> report(M message) {
    checkNotNull(message, "message");
    metrics.incrementMessages(1);

    if (closed.get()) { // Drop the message when closed.
      MessageDroppedException cause =
          dropped(new IllegalStateException("closed!"), singletonList(message));
      MessageFuture<R> future = (MessageFuture<R>) message.newFailedFuture(cause);
      metricsCallback(1);
      return future;
    }

    MessagePromise<R> promise = message.newPromise();
    emitter.onNext(promise);
    setLoggingListener(promise);
    return promise;
  }

  private void setLoggingListener(MessageFuture<R> future) {
    future.addListener(new FutureListener<R>() {

      @Override
      public void operationComplete(Future<R> future) {
        if (!future.isSuccess()) {
          MessageDroppedException cause = (MessageDroppedException) future.cause();
          metricsCallback(cause.dropped.size());
          logger.warn(cause.getMessage());
        }
      }
    });
  }

  @Override
  public void subscribe(FlowableEmitter<MessagePromise<R>> emitter) throws Exception {
    this.emitter = emitter;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) return;
    emitter.onComplete();
  }
}
