package io.github.tramchamploo.bufferslayer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import io.github.tramchamploo.bufferslayer.internal.Component;
import io.github.tramchamploo.bufferslayer.internal.MessageFuture;
import java.util.concurrent.TimeUnit;

/**
 * Reporter that reports messages to sender
 */
public interface Reporter<M extends Message, R> extends Component {

  /**
   * Schedules the message to be sent onto the transport.
   *
   * @param message Message, should not be <code>null</code>.
   */
  MessageFuture<R> report(M message);


  abstract class Builder<M extends Message, R> {

    final Sender<M, R> sender;
    ReporterMetrics metrics = ReporterMetrics.NOOP_METRICS;
    long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
    int bufferedMaxMessages = 100;
    int pendingMaxMessages = 10000;
    Strategy overflowStrategy = Strategy.DropHead;

    Builder(Sender<M, R> sender) {
      checkNotNull(sender, "sender");
      this.sender = sender;
    }

    public Builder<M, R> metrics(ReporterMetrics metrics) {
      this.metrics = checkNotNull(metrics, "metrics");
      return this;
    }

    public Builder<M, R> messageTimeout(long timeout, TimeUnit unit) {
      checkArgument(timeout >= 0, "timeout >= 0: %s", timeout);
      this.messageTimeoutNanos = unit.toNanos(timeout);
      return this;
    }

    public Builder<M, R> bufferedMaxMessages(int bufferedMaxMessages) {
      checkArgument(bufferedMaxMessages > 0, "bufferedMaxMessages > 0: %s", bufferedMaxMessages);
      this.bufferedMaxMessages = bufferedMaxMessages;
      return this;
    }

    public Builder<M, R> pendingMaxMessages(int pendingMaxMessages) {
      checkArgument(pendingMaxMessages > 0, "pendingMaxMessages > 0: %s", pendingMaxMessages);
      this.pendingMaxMessages = pendingMaxMessages;
      return this;
    }

    public Builder<M, R> overflowStrategy(Strategy overflowStrategy) {
      this.overflowStrategy = overflowStrategy;
      return this;
    }

    public abstract Reporter<M, R> build();
  }
}
