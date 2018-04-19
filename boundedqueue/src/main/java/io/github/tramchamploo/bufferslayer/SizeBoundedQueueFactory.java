package io.github.tramchamploo.bufferslayer;

import io.github.tramchamploo.bufferslayer.Message.MessageKey;
import io.github.tramchamploo.bufferslayer.OverflowStrategy.Strategy;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Base factory for {@link AbstractSizeBoundedQueue}
 * <p> This use {@link ServiceLoader} to find the actual factory
 */
public abstract class SizeBoundedQueueFactory {
  private static final SizeBoundedQueueFactory factory =
      load(SizeBoundedQueueFactory.class.getClassLoader());

  private static final SizeBoundedQueueFactory load(ClassLoader cl) {
    ServiceLoader<SizeBoundedQueueFactory> providers = ServiceLoader.load(SizeBoundedQueueFactory.class, cl);
    SizeBoundedQueueFactory best = null;

    for (SizeBoundedQueueFactory current : providers) {
      if (!current.isAvailable()) {
        continue;
      } else if (best == null) {
        best = current;
      } else if (current.priority() > best.priority()) {
        best = current;
      }
    }
    return best;
  }

  public static SizeBoundedQueueFactory factory() {
    if (factory == null) {
      throw new ServiceConfigurationError("No functional queue factory found.");
    }
    return factory;
  }

  /**
   * Return {@code true} if this factory is available else {@code false}
   */
  protected abstract boolean isAvailable();

  /**
   * Return priority of this factory, the higher the more possibility to be used
   */
  protected abstract int priority();

  protected abstract AbstractSizeBoundedQueue newQueue(int maxSize, Strategy overflowStrategy, MessageKey key);
}
