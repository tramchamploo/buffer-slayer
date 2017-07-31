package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.internal.SendingTask;
import io.vertx.ext.sql.UpdateResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.jdeferred.Promise;
import org.jdeferred.multiple.MasterDeferredObject;
import org.jdeferred.multiple.MasterProgress;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneReject;
import org.jdeferred.multiple.OneResult;

/**
 * Delegate sending and trigger succeed if specified otherwise failed with exception
 */
final class FakeAsyncSender implements AsyncSender<Statement, UpdateResult> {

  private AtomicBoolean closed = new AtomicBoolean(false);
  private Consumer<List<UpdateResult>> succeeded = null;
  private Supplier<Throwable> failed = null;

  private final AsyncSender<Statement, UpdateResult> delegate;

  FakeAsyncSender(AsyncSender<Statement, UpdateResult> delegate) {
    this.delegate = delegate;
  }

  @Override
  public CheckResult check() {
    return CheckResult.OK;
  }

  @Override
  public void close() throws IOException {
    closed.set(true);
    delegate.close();
  }

  @Override
  public Promise<MultipleResults, OneReject, MasterProgress> send(List<SendingTask<Statement>> tasks) {
    if (closed.get()) {
      throw new IllegalStateException("Closed!");
    }

    if (failed != null) {
      MasterDeferredObject deferred = new MasterDeferredObject();
      deferred.reject(new OneReject(0, null, failed.get()));
      return deferred.promise();
    } else {
      return delegate.send(tasks).done(result -> {
        if (succeeded != null) {
          succeeded.accept(toResults(result));
        }
      });
    }
  }

  private static List<UpdateResult> toResults(MultipleResults mr) {
    List<UpdateResult> results = new ArrayList<>(mr.size());
    for (Iterator<OneResult> iterator = mr.iterator(); iterator.hasNext(); ) {
      UpdateResult result = (UpdateResult) iterator.next().getResult();
      results.add(result);
    }
    return results;
  }

  public void succeeded(Consumer<List<UpdateResult>> succeeded) {
    this.succeeded = succeeded;
  }

  public void failed(Supplier<Throwable> failed) {
    this.failed = failed;
  }
}
