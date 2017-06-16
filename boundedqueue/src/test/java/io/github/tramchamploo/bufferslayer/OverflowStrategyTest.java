package io.github.tramchamploo.bufferslayer;

import static org.junit.Assert.assertEquals;

import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import org.junit.Assert;
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
    Assert.assertEquals(Strategy.DropHead, OverflowStrategy.create("DROPHEAD"));
    Assert.assertEquals(Strategy.DropTail, OverflowStrategy.create("DROPTAIL"));
    Assert.assertEquals(Strategy.DropNew, OverflowStrategy.create("DROPNEW"));
    Assert.assertEquals(Strategy.DropBuffer, OverflowStrategy.create("DROPBUFFER"));
    Assert.assertEquals(Strategy.Block, OverflowStrategy.create("block"));
    Assert.assertEquals(Strategy.Fail, OverflowStrategy.create("FAIL"));
  }
}
