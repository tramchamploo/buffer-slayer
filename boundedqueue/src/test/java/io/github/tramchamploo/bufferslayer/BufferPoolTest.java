package io.github.tramchamploo.bufferslayer;

import static io.github.tramchamploo.bufferslayer.Util.newSendingTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class BufferPoolTest {

  BufferPool bufferPool;

  @Test
  public void releaseDoNothingWhenFull() {
    bufferPool = new BufferPool(1, 1, false);
    Buffer one = bufferPool.acquire();
    Buffer another = bufferPool.acquire();
    bufferPool.release(one);
    bufferPool.release(another);
    assertEquals(1, bufferPool.buffersInPool);
    assertEquals(one, bufferPool.next);
    assertNull(bufferPool.next.next);
  }

  @Test
  public void shouldLinkBuffers() {
    bufferPool = new BufferPool(2, 1, false);
    Buffer<TestMessage> one = new Buffer<>(1, false);
    Buffer<TestMessage> another = new Buffer<>(1, false);
    one.accept(newSendingTask(0));
    another.accept(newSendingTask(0));

    bufferPool.release(one);
    bufferPool.release(another);

    assertEquals(2, bufferPool.buffersInPool);
    assertEquals(another, bufferPool.next);
    assertEquals(one, bufferPool.next.next);

    assertEquals(another, bufferPool.acquire());
    assertEquals(0, another.buffer.size());
    assertEquals(one, bufferPool.acquire());
    assertEquals(0, one.buffer.size());
  }
}
