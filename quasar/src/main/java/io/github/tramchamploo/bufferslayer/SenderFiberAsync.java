package io.github.tramchamploo.bufferslayer;

import co.paralleluniverse.common.util.CheckedCallable;
import co.paralleluniverse.fibers.FiberAsync;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import java.util.concurrent.ExecutorService;

abstract class SenderFiberAsync<V, E extends Throwable> extends FiberAsync<V, E> {

  @Suspendable
  static <V, E extends Exception> V exec(ExecutorService es, CheckedCallable<V, E> cc) throws E {
    try {
      return runBlocking(es, cc);
    } catch (final SuspendExecution se) {
      throw new AssertionError(se);
    } catch (final InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }
}
