package com.cosyan.db.model;

import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public abstract class ColumnMeta {

  protected final DataType<?> type;

  public abstract Object getValue(ImmutableMap<String, Object> sourceValues);

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class BasicColumn extends ColumnMeta {

    private final String name;

    public BasicColumn(String name, DataType<?> type) {
      super(type);
      this.name = name;
    }

    @Override
    public Object getValue(ImmutableMap<String, Object> sourceValues) {
      return sourceValues.get(name);
    }
  }

  public static abstract class DerivedColumn extends ColumnMeta {
    public DerivedColumn(DataType<?> type) {
      super(type);
    }
  }

  public static final DerivedColumn TRUE_COLUMN = new DerivedColumn(DataTypes.BoolType) {

    @Override
    public Object getValue(ImmutableMap<String, Object> sourceValues) {
      return true;
    }
  };
}
