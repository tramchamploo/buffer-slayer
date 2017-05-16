package io.bufferslayer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.util.List;
import org.jdeferred.multiple.MultipleResults;
import org.jdeferred.multiple.OneResult;

/**
 * Created by tramchamploo on 2017/4/14.
 */
class DeferredUtil {

  static List<Object> toResults(MultipleResults mr) {
    Builder<Object> builder = ImmutableList.builder();
    for (OneResult next : mr) {
      List<Object> batchResult = (List<Object>) next.getResult();
      builder.addAll(batchResult);
    }
    return builder.build();
  }
}
