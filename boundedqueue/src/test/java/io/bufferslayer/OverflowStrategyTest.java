package io.bufferslayer;

import static io.bufferslayer.OverflowStrategy.Strategy.DropBuffer;
import static io.bufferslayer.OverflowStrategy.Strategy.DropHead;
import static io.bufferslayer.OverflowStrategy.Strategy.DropNew;
import static io.bufferslayer.OverflowStrategy.Strategy.DropTail;
import static io.bufferslayer.OverflowStrategy.Strategy.Fail;
import static io.bufferslayer.OverflowStrategy.create;
import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by tramchamploo on 2017/4/18.
 */
public class OverflowStrategyTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createByName() {
    assertEquals(DropHead, create("DROPHEAD"));
    assertEquals(DropTail, create("DROPTAIL"));
    assertEquals(DropNew, create("DROPNEW"));
    assertEquals(DropBuffer, create("DROPBUFFER"));
    assertEquals(Fail, create("FAIL"));
  }
}
