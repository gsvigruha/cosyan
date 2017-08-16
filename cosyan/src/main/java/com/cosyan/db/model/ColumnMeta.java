package com.cosyan.db.model;

import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public abstract class ColumnMeta {

  protected final DataType<?> type;

  public abstract Object getValue(Object[] sourceValues);

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class BasicColumn extends ColumnMeta {

    private final int index;

    public BasicColumn(int index, DataType<?> type) {
      super(type);
      this.index = index;
    }

    @Override
    public Object getValue(Object[] sourceValues) {
      return sourceValues[index];
    }
  }

  public static abstract class DerivedColumn extends ColumnMeta {
    public DerivedColumn(DataType<?> type) {
      super(type);
    }
  }

  public static class AggrColumn extends DerivedColumn {

    private final int index;
    private final DerivedColumn baseColumn;
    private final TypedAggrFunction<?> function;

    public AggrColumn(DataType<?> type, DerivedColumn baseColumn, int index, TypedAggrFunction<?> function) {
      super(type);
      this.baseColumn = baseColumn;
      this.index = index;
      this.function = function;
    }

    @Override
    public Object getValue(Object[] sourceValues) {
      return sourceValues[index];
    }

    public Object getInnerValue(Object[] sourceValues) {
      return baseColumn.getValue(sourceValues);
    }

    public TypedAggrFunction<?> getFunction() {
      return function;
    }
  }

  public static class OrderColumn extends DerivedColumn {

    private final DerivedColumn baseColumn;
    private final boolean asc;

    public OrderColumn(DerivedColumn baseColumn, boolean asc) {
      super(baseColumn.type);
      this.baseColumn = baseColumn;
      this.asc = asc;
    }

    @Override
    public Object getValue(Object[] sourceValues) {
      return baseColumn.getValue(sourceValues);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public int compare(Object x, Object y) {
      return asc ? ((Comparable) x).compareTo(y) : ((Comparable) y).compareTo(x);
    }
  }

  public static final DerivedColumn TRUE_COLUMN = new DerivedColumn(DataTypes.BoolType) {

    @Override
    public Object getValue(Object[] sourceValues) {
      return true;
    }
  };
}
