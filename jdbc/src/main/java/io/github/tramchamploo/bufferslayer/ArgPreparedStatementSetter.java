package io.github.tramchamploo.bufferslayer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.springframework.jdbc.core.ParameterDisposer;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;

/**
 * Simple adapter for PreparedStatementSetter that applies
 * a given array of arguments.
 *
 * @author Juergen Hoeller
 */
class ArgPreparedStatementSetter implements PreparedStatementSetter, ParameterDisposer {

  private final Object[] args;


  /**
   * Create a new ArgPreparedStatementSetter for the given arguments.
   *
   * @param args the arguments to set
   */
  ArgPreparedStatementSetter(Object[] args) {
    this.args = args;
  }


  public void setValues(PreparedStatement ps) throws SQLException {
    if (this.args != null) {
      for (int i = 0; i < this.args.length; i++) {
        Object arg = this.args[i];
        if (arg instanceof SqlParameterValue) {
          SqlParameterValue paramValue = (SqlParameterValue) arg;
          StatementCreatorUtils.setParameterValue(ps, i + 1, paramValue, paramValue.getValue());
        } else {
          StatementCreatorUtils.setParameterValue(ps, i + 1, SqlTypeValue.TYPE_UNKNOWN, arg);
        }
      }
    }
  }

  public void cleanupParameters() {
    StatementCreatorUtils.cleanupParameters(this.args);
  }

}
