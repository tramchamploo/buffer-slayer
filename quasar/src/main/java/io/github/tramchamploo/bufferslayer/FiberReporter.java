package io.github.tramchamploo.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;

import co.paralleluniverse.concurrent.util.ScheduledSingleThreadExecutor;
import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.futures.AsyncCompletionStage;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.jdeferred.Promise;

public class FiberReporter<M extends Message, R> extends AsyncReporter<M, R> {

  private FiberReporter(AsyncReporter.Builder<M, R> builder) {
    super(builder);
  }

  public static <M extends Message, R> Builder<M, R> fiberBuilder(Sender<M, R> sender) {
    return new Builder<>(sender);
  }

  AsyncSender<M, R> toAsyncSender(AsyncReporter.Builder<M, R> builder) {
    return new FiberSenderAdaptor<>(builder.sender);
  }

  protected ScheduledExecutorService scheduler() {
    if (this.scheduler == null) {
      synchronized (this) {
        if (this.scheduler == null) {
          ThreadFactory timerFactory = new ThreadFactoryBuilder()
              .setNameFormat("FiberReporter-" + id + "-timer-%d")
              .setDaemon(true)
              .build();
          // Use quasar's ScheduledSingleThreadExecutor when timerThreads is 1.
          if (timerThreads == 1) {
            ScheduledSingleThreadExecutor singleThreadExecutor = new ScheduledSingleThreadExecutor(timerFactory);
            singleThreadExecutor.setRemoveOnCancelPolicy(true);
            this.scheduler = singleThreadExecutor;
          } else {
            ScheduledThreadPoolExecutor threadPoolExecutor = new ScheduledThreadPoolExecutor(timerThreads, timerFactory);
            threadPoolExecutor.setRemoveOnCancelPolicy(true);
            this.scheduler = threadPoolExecutor;
          }
        }
      }
    }
    return this.scheduler;
  }

  public static final class Builder<M extends Message, R> extends Reporter.Builder<Builder<M, R>, M, R> {

    int flushThreads = 1;
    int timerThreads = 1;
    long pendingKeepaliveNanos = TimeUnit.SECONDS.toNanos(60);
    boolean singleKey = false;

    Builder(Sender<M, R> sender) {
      super(sender);
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

    private AsyncReporter.Builder<M, R> asAsyncReporterBuilder() {
      return new AsyncReporter.Builder<>(sender)
          .timerThreads(timerThreads)
          .flushThreads(flushThreads)
          .singleKey(singleKey)
          .bufferedMaxMessages(bufferedMaxMessages)
          .pendingKeepalive(pendingKeepaliveNanos, TimeUnit.NANOSECONDS)
          .pendingMaxMessages(pendingMaxMessages)
          .messageTimeout(messageTimeoutNanos, TimeUnit.NANOSECONDS)
          .metrics(metrics)
          .overflowStrategy(overflowStrategy);
    }

    @Override
    public FiberReporter<M, R> build() {
      return new FiberReporter<>(asAsyncReporterBuilder());
    }
  }
}
