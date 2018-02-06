package com.cosyan.db.model;

import java.io.IOException;
import java.util.Set;

import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableSet;

import lombok.Data;

@Data
public abstract class ColumnMeta implements CompiledObject {

  protected final DataType<?> type;

  public abstract Object getValue(Object[] values, Resources resources) throws IOException;

  public abstract TableDependencies tableDependencies();

  public abstract MetaResources readResources();

  public abstract Set<TableMeta> tables();

  @Data
  public static class BasicColumn {

    private final DataType<?> type;
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
      this.type = type;
      this.index = index;
      this.name = name;
      this.nullable = nullable;
      this.unique = unique;
      this.indexed = indexed;
      this.deleted = false;
      assert !unique || indexed;
    }
  }

  public static class IndexColumn extends ColumnMeta {
    private final TableMeta sourceTable;
    private final int index;
    private final TableDependencies tableDependencies;

    public static IndexColumn of(TableMeta sourceTable, ColumnMeta parentColumn, int index) {
      return new IndexColumn(sourceTable, index, parentColumn.getType(), parentColumn.tableDependencies());
    }

    public IndexColumn(TableMeta sourceTable, int index, DataType<?> type, TableDependencies tableDependencies) {
      super(type);
      this.sourceTable = sourceTable;
      this.index = index;
      this.tableDependencies = tableDependencies;
    }

    @Override
    public Object getValue(Object[] values, Resources resources) throws IOException {
      return sourceTable.values(values, resources)[index];
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }

    @Override
    public TableDependencies tableDependencies() {
      return tableDependencies;
    }

    public int index() {
      return index;
    }

    public IndexColumn shift(TableMeta sourceTable, int shift) {
      return new IndexColumn(sourceTable, index + shift, type, tableDependencies);
    }

    @Override
    public Set<TableMeta> tables() {
      return ImmutableSet.of(sourceTable);
    }
  }

  public static abstract class DerivedColumn extends ColumnMeta {
    public DerivedColumn(DataType<?> type) {
      super(type);
    }
  }

  public static abstract class DerivedColumnWithDeps extends DerivedColumn {

    private final TableDependencies deps;
    private final MetaResources readResources;
    private final Set<TableMeta> tables;

    public DerivedColumnWithDeps(DataType<?> type, TableDependencies deps,
        MetaResources readResources, Set<TableMeta> tables) {
      super(type);
      this.deps = deps;
      this.readResources = readResources;
      this.tables = tables;
    }

    @Override
    public TableDependencies tableDependencies() {
      return deps;
    }

    @Override
    public MetaResources readResources() {
      return readResources;
    }

    @Override
    public Set<TableMeta> tables() {
      return tables;
    }
  }

  public static class AggrColumn extends DerivedColumn {

    private final AggrTables sourceTable;
    private final int index;
    private final ColumnMeta baseColumn;
    private final TypedAggrFunction<?> function;

    public AggrColumn(AggrTables sourceTable, DataType<?> type, ColumnMeta baseColumn, int index,
        TypedAggrFunction<?> function) {
      super(type);
      this.sourceTable = sourceTable;
      this.baseColumn = baseColumn;
      this.index = index;
      this.function = function;
    }

    @Override
    public Object getValue(Object[] sourceValues, Resources resources) throws IOException {
      return sourceTable.values(sourceValues, resources)[index];
    }

    public Object getInnerValue(Object[] values, Resources resources) throws IOException {
      return baseColumn.getValue(values, resources);
    }

    public TypedAggrFunction<?> getFunction() {
      return function;
    }

    @Override
    public MetaResources readResources() {
      return baseColumn.readResources();
    }

    @Override
    public TableDependencies tableDependencies() {
      return baseColumn.tableDependencies();
    }

    @Override
    public Set<TableMeta> tables() {
      return baseColumn.tables();
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
    public Object getValue(Object[] values, Resources resources) throws IOException {
      return baseColumn.getValue(values, resources);
    }

    @Override
    public MetaResources readResources() {
      return baseColumn.readResources();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public int compare(Object x, Object y) {
      return asc ? ((Comparable) x).compareTo(y) : ((Comparable) y).compareTo(x);
    }

    @Override
    public TableDependencies tableDependencies() {
      return baseColumn.tableDependencies();
    }

    @Override
    public Set<TableMeta> tables() {
      return baseColumn.tables();
    }
  }

  public static final DerivedColumn TRUE_COLUMN = new DerivedColumn(DataTypes.BoolType) {

    @Override
    public Object getValue(Object[] values, Resources resources) {
      return true;
    }

    @Override
    public TableDependencies tableDependencies() {
      return new TableDependencies();
    }

    @Override
    public MetaResources readResources() {
      return MetaResources.empty();
    }

    @Override
    public Set<TableMeta> tables() {
      return ImmutableSet.of();
    }
  };
}
