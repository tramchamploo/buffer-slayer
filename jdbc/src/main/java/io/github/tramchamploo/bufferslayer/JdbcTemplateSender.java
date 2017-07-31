package io.github.tramchamploo.bufferslayer;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Sender that delegates spring's {@link JdbcTemplate} to execute sqls.
 */
final class JdbcTemplateSender implements SyncSender<SQL, Integer> {

  private final JdbcTemplate underlying;

  JdbcTemplateSender(JdbcTemplate underlying) {
    this.underlying = underlying;
  }

  @Override
  public CheckResult check() {
    try {
      int one = underlying.queryForObject("SELECT 1;", Integer.class);
      return one == 1 ? CheckResult.OK
          : CheckResult.failed(new RuntimeException("SELECT 1 doesn't get 1."));
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  @Override
  public void close() throws IOException {
  }

  public List<Integer> send(List<SQL> sqls) {
    Preconditions.checkArgument(sqls != null && sqls.size() > 0, "SQLs should not be empty.");
    if (!sameSQL(sqls)) {
      throw new UnsupportedOperationException("Different sqls are not supported");
    }

    boolean prepared = allPreparedStatement(sqls);
    int[] rowsAffected;
    if (prepared) {
      rowsAffected = underlying.batchUpdate(sqls.get(0).sql, batchPreparedStatementSetter(sqls));
    } else {
      rowsAffected = underlying.batchUpdate(extractSQLs(sqls).toArray(new String[0]));
    }

    List<Integer> ret = new ArrayList<>(rowsAffected.length);
    for (int aRowsAffected : rowsAffected) {
      ret.add(aRowsAffected);
    }
    return ret;
  }

  private static List<String> extractSQLs(List<SQL> sqls) {
    ArrayList<String> ret = new ArrayList<>(sqls.size());
    for (SQL s : sqls) {
      ret.add(s.sql);
    }
    return ret;
  }

  private static BatchPreparedStatementSetter batchPreparedStatementSetter(final List<SQL> sqls) {
    return new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        sqls.get(i).statementSetter.setValues(ps);
      }

      @Override
      public int getBatchSize() {
        return sqls.size();
      }
    };
  }

  private static boolean sameSQL(List<SQL> sqls) {
    if (sqls.size() == 1) {
      return true;
    }
    Iterator<SQL> iter = sqls.iterator();
    String first = iter.next().sql;
    while (iter.hasNext()) {
      if (!first.equals(iter.next().sql)) {
        return false;
      }
    }
    return true;
  }

  private static boolean allPreparedStatement(List<SQL> sqls) {
    if (sqls.size() == 1) {
      return sqls.get(0).prepared();
    }
    Iterator<SQL> iter = sqls.iterator();
    boolean prepared = iter.next().prepared();
    while (iter.hasNext()) {
      if (prepared != (iter.next().prepared())) {
        throw new UnsupportedOperationException("All messages must be either prepared or not.");
      }
    }
    return prepared;
  }
}
