package io.github.tramchamploo.bufferslayer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;

public class BatchMySQLClient implements MySQLClient {

  private final Vertx vertx;
  private final AsyncSQLClient underlying;
  private final Reporter<Statement, UpdateResult> reporter;

  private BatchMySQLClient(
      Vertx vertx,
      AsyncSQLClient underlying,
      Reporter<Statement, UpdateResult> reporter
  ) {
    this.vertx = vertx;
    this.underlying = underlying;
    this.reporter = reporter;
  }

  // visible for tests
  static AsyncSQLClient wrap(Vertx vertx, AsyncSQLClient client, Reporter<Statement, UpdateResult> reporter) {
    return new BatchMySQLClient(vertx, client, reporter);
  }

  @SuppressWarnings("unchecked")
  public static AsyncSQLClient wrap(Vertx vertx, AsyncSQLClient client, ReporterProperties properties) {
    return new BatchMySQLClient(vertx, client, properties.setSender(new SQLConnectionSender(client)).toBuilder().build());
  }

  @Override
  public SQLClient getConnection(Handler<AsyncResult<SQLConnection>> handler) {
    underlying.getConnection(get -> {
      if (get.succeeded()) {
        SQLConnection realConnection = get.result();
        handler.handle(Future.succeededFuture(BatchSQLConnection.wrap(vertx.getOrCreateContext(), realConnection, reporter)));
      } else {
        handler.handle(Future.failedFuture(get.cause()));
      }
    });
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    underlying.close(completionHandler);
  }

  @Override
  public void close() {
    underlying.close();
  }
}
