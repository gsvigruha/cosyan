package com.cosyan.db.model;

import java.io.IOException;

import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Dependencies.TableDependencies;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public abstract class ColumnMeta implements CompiledObject {

  protected final DataType<?> type;

  public abstract Object getValue(SourceValues values) throws IOException;

  public abstract TableDependencies tableDependencies();

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class BasicColumn extends ColumnMeta {

    private final int index;
    private final String name;
    private boolean nullable;
    private boolean unique;
    private boolean indexed;
    private boolean deleted;

    public BasicColumn(int index, String name, DataType<?> type) {
      this(index, name, type, true, false, false);
    }

    public BasicColumn(
        int index,
        String name,
        DataType<?> type,
        boolean nullable,
        boolean unique) {
      this(index, name, type, nullable, unique, /* indexed= */unique);
    }

    public BasicColumn(
        int index,
        String name,
        DataType<?> type,
        boolean nullable,
        boolean unique,
        boolean indexed) {
      super(type);
      this.index = index;
      this.name = name;
      this.nullable = nullable;
      this.unique = unique;
      this.indexed = indexed;
      this.deleted = false;
      assert !unique || indexed;
    }

    @Override
    public Object getValue(SourceValues values) {
      return values.sourceValue(index);
    }

    @Override
    public TableDependencies tableDependencies() {
      return new TableDependencies();
    }
  }

  public static class IterableColumn extends ColumnMeta {

    private final ColumnMeta sourceColumn;

    public IterableColumn(ColumnMeta sourceColumn) throws ModelException {
      super(sourceColumn.getType().toListType());
      this.sourceColumn = sourceColumn;
    }

    @Override
    public Object getValue(SourceValues values) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TableDependencies tableDependencies() {
      return sourceColumn.tableDependencies();
    }
  }

  public static abstract class DerivedColumn extends ColumnMeta {
    public DerivedColumn(DataType<?> type) {
      super(type);
    }
  }

  public static abstract class DerivedColumnWithDeps extends DerivedColumn {

    private final TableDependencies deps;

    public DerivedColumnWithDeps(DataType<?> type, TableDependencies deps) {
      super(type);
      this.deps = deps;
    }

    @Override
    public TableDependencies tableDependencies() {
      return deps;
    }
  }

  public static class AggrColumn extends DerivedColumn {

    private final int index;
    private final ColumnMeta baseColumn;
    private final TypedAggrFunction<?> function;

    public AggrColumn(DataType<?> type, ColumnMeta baseColumn, int index, TypedAggrFunction<?> function) {
      super(type);
      this.baseColumn = baseColumn;
      this.index = index;
      this.function = function;
    }

    @Override
    public Object getValue(SourceValues values) {
      return values.sourceValue(index);
    }

    public Object getInnerValue(SourceValues values) throws IOException {
      return baseColumn.getValue(values);
    }

    public TypedAggrFunction<?> getFunction() {
      return function;
    }

    @Override
    public TableDependencies tableDependencies() {
      return baseColumn.tableDependencies();
    }
  }

  public static class OrderColumn extends DerivedColumn {

    private final ColumnMeta baseColumn;
    private final boolean asc;

    public OrderColumn(ColumnMeta baseColumn, boolean asc) {
      super(baseColumn.type);
      this.baseColumn = baseColumn;
      this.asc = asc;
    }

    @Override
    public Object getValue(SourceValues values) throws IOException {
      return baseColumn.getValue(values);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public int compare(Object x, Object y) {
      return asc ? ((Comparable) x).compareTo(y) : ((Comparable) y).compareTo(x);
    }

    @Override
    public TableDependencies tableDependencies() {
      return baseColumn.tableDependencies();
    }
  }

  public static final DerivedColumn TRUE_COLUMN = new DerivedColumn(DataTypes.BoolType) {

    @Override
    public Object getValue(SourceValues values) {
      return true;
    }

    @Override
    public TableDependencies tableDependencies() {
      return new TableDependencies();
    }
  };
}
