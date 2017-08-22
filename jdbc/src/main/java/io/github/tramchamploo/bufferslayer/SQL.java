package io.github.tramchamploo.bufferslayer;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.ArgumentTypePreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementSetter;

/**
 * Represent sql to be sent
 */
public final class SQL extends Message {

  final String sql;
  transient final PreparedStatementSetter statementSetter;
  transient Object[] args;
  transient final int[] argTypes;

  static Builder builder() {
    return new Builder();
  }

  SQL(Builder builder) {
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
    if (prepared() && statementSetter instanceof ArgumentPreparedStatementSetter) {
      return String.format("sql: %s, args: %s", sql, Arrays.toString(args));
    } else if (prepared() && statementSetter instanceof ArgumentTypePreparedStatementSetter) {
      return String.format("sql: %s, args: %s, argTypes: %s", sql, Arrays.toString(args),
          Arrays.toString(argTypes));
    }
    return "sql: " + sql;
  }

  private void writeObject(ObjectOutputStream s) throws IOException {
    s.defaultWriteObject();
    // write length of args
    s.writeInt(args.length);
    // write args
    if (args.length > 0) {
      for (Object arg : args) {
        s.writeObject(arg);
      }
    }
  }

  private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException  {
    s.defaultReadObject();
    // read length of args
    int argCount = s.readInt();
    // read args
    args = new Object[argCount];
    for (int i = 0; i < argCount; i++) {
      args[i] = s.readObject();
    }
  }

  static final class Builder {

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
        this.preparedStatementSetter = new ArgumentTypePreparedStatementSetter(args, argTypes);
      } else {
        this.preparedStatementSetter = new ArgumentPreparedStatementSetter(args);
      }
      return this;
    }

    Builder argTypes(int[] argTypes) {
      this.argTypes = argTypes;
      if (args != null) {
        this.preparedStatementSetter = new ArgumentTypePreparedStatementSetter(args, argTypes);
      }
      return this;
    }

    SQL build() {
      Preconditions.checkArgument(sql != null, "SQL must not be null.");
      return new SQL(this);
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

    @Override
    public String toString() {
      return "SqlKey(" + sql + ", " + prepared + ")";
    }
  }
}
