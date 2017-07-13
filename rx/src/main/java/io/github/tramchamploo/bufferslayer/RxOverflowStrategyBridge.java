package io.github.tramchamploo.bufferslayer;

import io.reactivex.BackpressureOverflowStrategy;

final class RxOverflowStrategyBridge {

  /**
   * Convert a {@link OverflowStrategy.Strategy} to rx-java's {@link BackpressureOverflowStrategy}
   */
  static BackpressureOverflowStrategy toRxStrategy(OverflowStrategy.Strategy strategy) {
    switch (strategy) {
      case Fail:
        return BackpressureOverflowStrategy.ERROR;
      case DropTail:
        return BackpressureOverflowStrategy.DROP_LATEST;
      case DropHead:
        return BackpressureOverflowStrategy.DROP_OLDEST;
      default:
        throw new UnsupportedOperationException(strategy + " not supported using rx-java.");
    }
  }
}
