package io.bufferslayer;

import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Created by guohang.bao on 2017/4/14.
 */
final class DefaultSenderToAsyncSenderAdaptor<M extends Message, R> extends
    SenderToAsyncSenderAdaptor<M, R> {

  private final int parallelism;

  DefaultSenderToAsyncSenderAdaptor(Sender<M, R> delegate, Executor executor, int parallelism) {
    super(delegate, executor);
    this.parallelism = executor == MoreExecutors.directExecutor() ? 1 : parallelism;
  }

  @Override
  protected List<List<M>> split(List<M> messages) {
    if (parallelism == 1) return Collections.singletonList(messages); // Do not split

    int size = messages.size();
    int step = size / parallelism;
    step = step == 0 ? 1 : step; // size is less than nPieces

    List<List<M>> result = new ArrayList<>(parallelism);
    for (int i = 0; i < messages.size(); i += step) {
      if (i + step >= messages.size() || result.size() + 1 == parallelism) {
        result.add(messages.subList(i, messages.size()));
        break;
      }
      result.add(messages.subList(i, i + step));
    }
    return result;
  }
}
