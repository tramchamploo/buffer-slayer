package io.github.tramchamploo.bufferslayer;

import static com.google.common.base.Preconditions.checkNotNull;

import io.vertx.core.json.JsonArray;

/**
 * Represent sql to be sent
 */
public final class Statement extends Message {

  final String sql;
  final JsonArray args;

  static Builder builder() {
    return new Builder();
  }

  Statement(Builder builder) {
    this.sql = builder.sql;
    this.args = builder.args;
  }

  boolean withArgs() {
    return args != null;
  }

  @Override
  public MessageKey asMessageKey() {
    return new StatementKey(sql, withArgs());
  }

  @Override
  public String toString() {
    if (withArgs()) {
      return String.format("sql: %s, args: %s", sql, args.toString());
    }
    return "sql: " + sql;
  }

  static final class Builder {

    private String sql;
    private JsonArray args;

    Builder sql(String sql) {
      this.sql = checkNotNull(sql);
      return this;
    }

    Builder args(JsonArray args) {
      this.args = checkNotNull(args);
      return this;
    }

    Statement build() {
      return new Statement(this);
    }
  }

  public static final class StatementKey extends MessageKey {

    final String sql;
    final boolean prepared;

    StatementKey(String sql, boolean prepared) {
      this.sql = sql;
      this.prepared = prepared;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      StatementKey statementKey = (StatementKey) o;
      if (!this.prepared && this.prepared == statementKey.prepared) {
        return true;
      }
      return sql != null ? sql.equals(statementKey.sql) : statementKey.sql == null;
    }

    @Override
    public int hashCode() {
      if (!this.prepared) {
        return 0;
      }
      return sql != null ? sql.hashCode() : 0;
    }

    @Override
    public String toString() {
      return "StatementKey(" + sql + ", " + prepared + ")";
    }
  }
}
