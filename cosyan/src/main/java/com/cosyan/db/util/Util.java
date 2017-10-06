package com.cosyan.db.util;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

public class Util {

  public static <K, V> ImmutableMap<K, V> merge(
      ImmutableMap<K, V> map1,
      ImmutableMap<K, V> map2,
      BiFunction<? super V, ? super V, ? extends V> mergeFunction) {
    ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
    for (Map.Entry<K, V> entry : map1.entrySet()) {
      if (!map2.containsKey(entry.getKey())) {
        builder.put(entry);
      } else {
        builder.put(entry.getKey(), mergeFunction.apply(entry.getValue(), map2.get(entry.getKey())));
      }
    }
    for (Map.Entry<K, V> entry : map2.entrySet()) {
      if (!map1.containsKey(entry.getKey())) {
        builder.put(entry);
      }
    }
    return builder.build();
  }

  public static <K, V> ImmutableMap<K, V> merge(
      Iterable<ImmutableMap<K, V>> maps,
      BiFunction<? super V, ? super V, ? extends V> mergeFunction) {
    Map<K, V> merged = new LinkedHashMap<>();
    for (ImmutableMap<K, V> map : maps) {
      for (Map.Entry<K, V> entry : map.entrySet()) {
        if (merged.containsKey(entry.getKey())) {
          merged.put(entry.getKey(), mergeFunction.apply(merged.get(entry.getKey()), entry.getValue()));
        } else {
          merged.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return ImmutableMap.copyOf(merged);
  }

  @FunctionalInterface
  public interface CheckedFunction<T, R> {
    R apply(T t) throws IOException;
  }

  public static <K, U, V> ImmutableMap<K, V> mapValues(
      Map<K, U> map,
      Function<U, V> mapFunction) {
    ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
    for (Map.Entry<K, U> entry : map.entrySet()) {
      builder.put(entry.getKey(), mapFunction.apply(entry.getValue()));
    }
    return builder.build();
  }

  public static <K, U, V> ImmutableMap<K, V> mapValuesIOException(
      Map<K, U> map,
      CheckedFunction<U, V> mapFunction) throws IOException {
    ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
    for (Map.Entry<K, U> entry : map.entrySet()) {
      builder.put(entry.getKey(), mapFunction.apply(entry.getValue()));
    }
    return builder.build();
  }
}
