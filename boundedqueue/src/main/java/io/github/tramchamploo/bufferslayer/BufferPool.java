package io.github.tramchamploo.bufferslayer;

/**
 * A buffer pool which keeps a free list of direct buffers of a specified default
 * size in a simple fixed size stack.
 */
final class BufferPool {

  Buffer next;
  int buffersInPool = 0;
  private int maxPoolEntries;
  private int bufferSize;
  private boolean onlyAcceptSame;

  BufferPool(int maxPoolEntries, int bufferSize, boolean onlyAcceptSame) {
    this.maxPoolEntries = maxPoolEntries;
    this.bufferSize = bufferSize;
    this.onlyAcceptSame = onlyAcceptSame;
  }

  /**
   * Acquire a buffer from the pool
   */
  Buffer acquire() {
    synchronized (this) {
      if (next != null) {
        Buffer result = next;
        next = result.next;
        result.next = null;
        buffersInPool--;
        result.clear();
        return result;
      }
    }
    return new Buffer(bufferSize, onlyAcceptSame);
  }

  /**
   * Return the buffer back to pool
   * @param buffer buffer to return to pool
   */
  void release(Buffer buffer) {
    if (buffer.next != null) throw new IllegalArgumentException();
    synchronized (this) {
      if (buffersInPool == maxPoolEntries) return; // Pool is full.
      buffersInPool++;
      buffer.next = next;
      next = buffer;
    }
  }
}
