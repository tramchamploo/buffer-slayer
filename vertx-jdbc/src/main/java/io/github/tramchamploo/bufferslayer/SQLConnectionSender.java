package io.github.tramchamploo.bufferslayer;

import com.google.common.base.Preconditions;
import io.github.tramchamploo.bufferslayer.internal.Deferreds;
import io.github.tramchamploo.bufferslayer.internal.SendingTask;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.jdeferred.Deferred;
import org.jdeferred.Promise;
import org.jdeferred.multiple.MasterDeferredObject;
import org.jdeferred.multiple.MasterProgress;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneReject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute sqls using vertx async mysql client
 */
final class SQLConnectionSender implements AsyncSender<Statement, UpdateResult> {

  private static final Logger logger = LoggerFactory.getLogger(AsyncReporter.class);

  private static final String KEY_SENDER_CONNECTIONS = "bufferslayer.sender-connections";

  private final AsyncSQLClient underlying;
  private final BlockingQueue<SQLConnection> connections;

  SQLConnectionSender(AsyncSQLClient underlying) {
    this.underlying = underlying;
    int poolSize = Math.max(1, Integer.getInteger(KEY_SENDER_CONNECTIONS, 3));
    logger.info("Will hold {} SQLConnections", poolSize);

    connections = new ArrayBlockingQueue<>(poolSize);
    for (int i = 0; i < poolSize; i++) {
      underlying.getConnection(get -> {
        if (get.succeeded()) {
          connections.offer(get.result());
        }
      });
    }
  }

  private void getConnection(Handler<AsyncResult<SQLConnection>> handler) {
    try {
      SQLConnection conn = connections.take();
      handler.handle(Future.succeededFuture(conn));
    } catch (InterruptedException e) {
      handler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public Promise<MultipleResults, OneReject, MasterProgress> send(List<SendingTask<Statement>> tasks) {
    Preconditions.checkNotNull(tasks);

    Object[] statementAndDeferred = SendingTask.unzipGeneric(tasks);
    @SuppressWarnings("unchecked")
    final List<Statement> statements = (List<Statement>) statementAndDeferred[0];
    @SuppressWarnings("unchecked")
    final List<Deferred> deferreds = (List<Deferred>) statementAndDeferred[1];

    if (!sameSQL(statements)) {
      throw new UnsupportedOperationException("Different SQLs are not supported");
    }

    boolean allWithArgs = allWithArgs(statements);
    MasterDeferredObject deferred = new MasterDeferredObject(deferreds.toArray(new Deferred[0]));

    getConnection(connection -> {
      SQLConnection conn = connection.result();
      Handler<AsyncResult<List<Integer>>> handler = result -> {
        try {
          if (result.succeeded()) {
            Deferreds.resolveAll(toUpdateResults(result.result()), deferreds);
          } else {
            Deferreds.rejectAll(MessageDroppedException.dropped(result.cause(), statements), deferreds, statements);
          }
        } finally {
          connections.offer(conn);
        }
      };

      if (allWithArgs) {
        conn.batchWithParams(statements.get(0).sql, collectArgs(statements), handler);
      } else {
        conn.batch(collectSQLs(statements), handler);
      }
    });

    return deferred.promise();
  }

  private static List<String> collectSQLs(List<Statement> statements) {
    ArrayList<String> ret = new ArrayList<>(statements.size());
    for (Statement s : statements) {
      ret.add(s.sql);
    }
    return ret;
  }

  private static List<UpdateResult> toUpdateResults(List<Integer> rowsUpdated) {
    List<UpdateResult> result = new ArrayList<>(rowsUpdated.size());
    for (Integer updated: rowsUpdated) {
      result.add(new UpdateResult(updated, null));
    }
    return result;
  }

  private static List<JsonArray> collectArgs(List<Statement> statements) {
    ArrayList<JsonArray> ret = new ArrayList<>(statements.size());
    for (Statement s : statements) {
      ret.add(s.args);
    }
    return ret;
  }

  private static boolean sameSQL(List<Statement> statements) {
    if (statements.size() == 1) {
      return true;
    }
    Iterator<Statement> iter = statements.iterator();
    String first = iter.next().sql;
    while (iter.hasNext()) {
      if (!first.equals(iter.next().sql)) {
        return false;
      }
    }
    return true;
  }

  private static boolean allWithArgs(List<Statement> statements) {
    if (statements.size() == 1) {
      return statements.get(0).withArgs();
    }
    Iterator<Statement> iter = statements.iterator();
    boolean prepared = iter.next().withArgs();
    while (iter.hasNext()) {
      if (prepared != (iter.next().withArgs())) {
        throw new UnsupportedOperationException("All messages must be either withArgs or not.");
      }
    }
    return prepared;
  }

  @Override
  public CheckResult check() {
    CountDownLatch countDown = new CountDownLatch(1);
    AtomicReference<CheckResult> checkResult = new AtomicReference<>();

    getConnection(connection -> {
      SQLConnection conn = connection.result();
      conn.query("SELECT 1;", result -> {
        try {
          if (result.succeeded() && result.result().getNumRows() == 1) {
            checkResult.set(CheckResult.OK);
          } else {
            checkResult.set(CheckResult.failed((Exception) result.cause()));
          }
        } finally {
          conn.close();
        }
        countDown.countDown();
      });
    });
    return checkResult.get();
  }

  @Override
  public void close() throws IOException {
    connections.forEach(SQLConnection::close);
  }
}
