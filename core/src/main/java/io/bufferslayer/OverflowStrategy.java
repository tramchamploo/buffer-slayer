package io.bufferslayer;

import static io.bufferslayer.OverflowStrategy.Strategy.Block;
import static io.bufferslayer.OverflowStrategy.Strategy.DropBuffer;
import static io.bufferslayer.OverflowStrategy.Strategy.DropHead;
import static io.bufferslayer.OverflowStrategy.Strategy.DropNew;
import static io.bufferslayer.OverflowStrategy.Strategy.DropTail;
import static io.bufferslayer.OverflowStrategy.Strategy.Fail;

/**
 * Created by tramchamploo on 2017/4/10.
 * Represents a strategy that decides how to deal with a buffer that is full but is
 * about to receive a new element.
 * This is inspired by Akka stream.
 */
public abstract class OverflowStrategy {

  /**
   * create by name
   */
  public static Strategy create(String name) {
    name = name.toLowerCase();
    if (name.equals(DropHead.name().toLowerCase())) {
      return DropHead;
    } else if (name.equals(DropTail.name().toLowerCase())) {
      return DropTail;
    } else if (name.equals(DropBuffer.name().toLowerCase())) {
      return DropBuffer;
    } else if (name.equals(DropNew.name().toLowerCase())) {
      return DropNew;
    } else if (name.equals(Block.name().toLowerCase())) {
      return Block;
    } else if (name.equals(Fail.name().toLowerCase())) {
      return Fail;
    }
    throw new UnsupportedOperationException(name);
  }

  enum Strategy {
    DropHead, DropTail, DropBuffer, DropNew, Block, Fail
  }

  /**
   * If the buffer is full when a new element arrives, drops the oldest element from the buffer to make space for
   * the new element.
   */
  public static final Strategy dropHead = Strategy.DropHead;

  /**
   * If the buffer is full when a new element arrives, drops the youngest element from the buffer to make space for
   * the new element.
   */
  public static final Strategy dropTail = Strategy.DropTail;

  /**
   * If the buffer is full when a new element arrives, drops all the buffered elements to make space for the new element.
   */
  public static final Strategy dropBuffer = Strategy.DropBuffer;

  /**
   * If the buffer is full when a new element arrives, drops the new element.
   */
  public static final Strategy dropNew = Strategy.DropNew;

  /**
   * If the buffer is full when a new element is arrives, blocks the caller thread.
   */
  public static final Strategy block = Strategy.Block;

  /**
   * If the buffer is full when a new element arrives, throws an exception.
   */
  public static final Strategy fail = Strategy.Fail;
}
