package io.github.tramchamploo.bufferslayer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Test;

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

  @Test
  public void unpreparedStatementsShareTheSameKey() {
    Sql sql = Sql.builder()
        .sql("insert into unprepared values(1, 1)")
        .build();

    Sql sql2 = Sql.builder()
        .sql("insert into unprepared values(2, 2)")
        .build();

    assertEquals(sql.asMessageKey(), sql2.asMessageKey());
  }

  @Test
  public void samePreparedStatementsShareTheSameKey() {
    Sql sql = Sql.builder()
        .sql("insert into prepared values(?, ?)")
        .args(new Object[]{1, 1})
        .build();

    Sql sql2 = Sql.builder()
        .sql("insert into prepared values(?, ?)")
        .args(new Object[]{2, 2})
        .build();

    assertEquals(sql.asMessageKey(), sql2.asMessageKey());
  }

  @Test
  public void differentPreparedStatementsHaveDifferentKeys() {
    Sql sql = Sql.builder()
        .sql("insert into one values(?, ?)")
        .args(new Object[]{1, 1})
        .build();

    Sql sql2 = Sql.builder()
        .sql("insert into two values(?, ?)")
        .args(new Object[]{2, 2})
        .build();

    assertNotEquals(sql.asMessageKey(), sql2.asMessageKey());
  }

  @Test
  public void deserialize() throws Exception {
    String sql = "to serialize";
    Object[] args = {"arg1", "arg2"};
    Sql toSerialize = Sql.builder()
        .sql(sql)
        .args(args)
        .build();

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bytes);
    oos.writeObject(toSerialize);
    oos.flush();
    byte[] buf = bytes.toByteArray();

    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buf));
    Sql deserialized = (Sql) ois.readObject();
    assertEquals(sql, deserialized.sql);
    assertArrayEquals(args, deserialized.args);
  }
}
