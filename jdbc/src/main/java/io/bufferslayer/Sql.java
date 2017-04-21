package io.bufferslayer;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.springframework.jdbc.core.PreparedStatementSetter;

/**
 * Created by guohang.bao on 2017/2/27.
 * Represent sql to be buffered
 */
final class Sql extends Message {

  final String sql;
  final PreparedStatementSetter statementSetter;
  final Object[] args;
  final int[] argTypes;

  static Builder builder() {
    return new Builder();
  }

  Sql(Builder builder) {
    this.sql = builder.sql;
    this.statementSetter = builder.preparedStatementSetter;
    this.args = builder.args;
    this.argTypes = builder.argTypes;
  }

  boolean prepared() {
    return statementSetter != null;
  }

  @Override
  public MessageKey asMessageKey() {
    return new SqlKey(sql, prepared());
  }

  @Override
  public String toString() {
    if (prepared() && statementSetter instanceof ArgPreparedStatementSetter) {
      return String.format("sql: %s, args: %s", sql, Arrays.toString(args));
    } else if (prepared() && statementSetter instanceof ArgTypePreparedStatementSetter) {
      return String.format("sql: %s, args: %s, argTypes: %s", sql, Arrays.toString(args),
          Arrays.toString(argTypes));
    }
    return "sql: " + sql;
  }

  public static final class Builder {

    private String sql;
    private PreparedStatementSetter preparedStatementSetter;
    private Object[] args;
    private int[] argTypes;

    Builder sql(String sql) {
      this.sql = sql;
      return this;
    }

    Builder preparedStatementSetter(PreparedStatementSetter pss) {
      this.preparedStatementSetter = pss;
      return this;
    }

    Builder args(Object[] args) {
      this.args = args;
      if (argTypes != null) {
        this.preparedStatementSetter = new ArgTypePreparedStatementSetter(args, argTypes);
      } else {
        this.preparedStatementSetter = new ArgPreparedStatementSetter(args);
      }
      return this;
    }

    Builder argTypes(int[] argTypes) {
      this.argTypes = argTypes;
      if (args != null) {
        this.preparedStatementSetter = new ArgTypePreparedStatementSetter(args, argTypes);
      }
      return this;
    }

    Sql build() {
      Preconditions.checkArgument(sql != null, "Sql must not be null.");
      return new Sql(this);
    }
  }

  public static final class SqlKey extends MessageKey {

    final String sql;
    final boolean prepared;

    SqlKey(String sql, boolean prepared) {
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

      SqlKey sqlKey = (SqlKey) o;
      if (!this.prepared && this.prepared == sqlKey.prepared) {
        return true;
      }
      return sql != null ? sql.equals(sqlKey.sql) : sqlKey.sql == null;
    }

    @Override
    public int hashCode() {
      if (!this.prepared) {
        return 0;
      }
      return sql != null ? sql.hashCode() : 0;
    }
  }
}
