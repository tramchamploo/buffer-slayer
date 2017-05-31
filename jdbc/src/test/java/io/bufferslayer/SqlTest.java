package io.bufferslayer;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Created by tramchamploo on 2017/5/31.
 */
public class SqlTest {

  @Test
  public void sqlKeyToString() {
    Sql sql = Sql.builder()
        .sql("insert into prepared values(?, ?)")
        .args(new Object[]{0, 1})
        .build();
    assertEquals("SqlKey(insert into prepared values(?, ?), true)",
        sql.asMessageKey().toString());

    sql = Sql.builder()
        .sql("insert into unprepared values(0, 1)")
        .build();
    assertEquals("SqlKey(insert into unprepared values(0, 1), false)",
        sql.asMessageKey().toString());
  }
}
