package com.cosyan.db.model;

import com.google.common.collect.ImmutableMap;

public class TableContext {

  public static final TableContext EMPTY = new TableContext(ImmutableMap.of());

  public static final String PARENT = "PARENT";

  public static TableContext withParent(Object[] values) {
    return new TableContext(ImmutableMap.of(PARENT, values));
  }

  private final ImmutableMap<String, Object[]> parentValues;

  public TableContext(ImmutableMap<String, Object[]> parentValues) {
    this.parentValues = parentValues;
  }

  public Object[] values(String key) {
    return parentValues.get(key);
  }
}