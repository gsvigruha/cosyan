package com.cosyan.db.model;

import java.util.Map;

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
}
