package com.cosyan.db.model;

import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SourceValues {

  private final Object[] sourceValues;
  private final ImmutableMap<String, Object[]> referencedValues;

  public SourceValues(Object[] sourceValues, ImmutableMap<String, Object[]> referencedValues) {
    this.sourceValues = sourceValues;
    this.referencedValues = referencedValues;
  }

  public Object sourceValue(int index) {
    return sourceValues[index];
  }

  public Object refTableValue(String tableName, int index) {
    return referencedValues.get(tableName)[index];
  }

  public static SourceValues of(Object[] sourceValues) {
    return new SourceValues(sourceValues, ImmutableMap.of());
  }

  public static SourceValues of(Object[] sourceValues, Map<String, Object[]> referencedValues) {
    return new SourceValues(sourceValues, ImmutableMap.copyOf(referencedValues));
  }

  public boolean isEmpty() {
    return false;
  }

  private static class EmptySourceValues extends SourceValues {

    public EmptySourceValues() {
      super(new Object[] {}, ImmutableMap.of());
    }

    @Override
    public boolean isEmpty() {
      return true;
    }
  }

  public static final SourceValues EMPTY = new EmptySourceValues();

  public ImmutableList<Object> toList() {
    return ImmutableList.copyOf(sourceValues);
  }

  public Object[] toArray() {
    return sourceValues;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(ImmutableList.copyOf(sourceValues).stream().map(o -> o.toString()).collect(Collectors.joining(", ")));
    sb.append("\n");
    for (Map.Entry<String, Object[]> dep : referencedValues.entrySet()) {
      sb.append(" " + dep.getKey() + " " + ImmutableList.copyOf(dep.getValue()).stream().map(o -> o.toString()).collect(Collectors.joining(", ")));
    }
    return sb.toString();
  }
}
