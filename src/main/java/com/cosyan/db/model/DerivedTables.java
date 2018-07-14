package com.cosyan.db.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.TreeMap;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.DerivedIterableTableReader;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableReader.MultiFilteredTableReader;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableMeta.IterableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DerivedTables {

  protected static MetaResources resourcesFromColumns(Iterable<? extends ColumnMeta> columns) {
    MetaResources resources = MetaResources.empty();
    for (ColumnMeta columnMeta : columns) {
      resources = resources.merge(columnMeta.readResources());
    }
    return resources;
  }

  private static MetaResources resourcesFromColumn(ColumnMeta column) {
    return MaterializedTable.readResources(column.tableDependencies().getDeps().values());
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DerivedTableMeta extends ExposedTableMeta {
    private final IterableTableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> columns;

    @Override
    public ImmutableList<String> columnNames() {
      return columns.keySet().asList();
    }

    @Override
    public ImmutableList<DataType<?>> columnTypes() {
      return columns.values().stream().map(c -> c.getType()).collect(ImmutableList.toImmutableList());
    }

    @Override
    public IndexColumn getColumn(Ident ident) throws ModelException {
      ColumnMeta column = columns.get(ident.getString());
      if (column == null) {
        return null;
      }
      return IndexColumn.of(this, column, indexOf(columns.keySet(), ident));
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return null;
    }

    @Override
    public MetaResources readResources() {
      MetaResources resources = resourcesFromColumns(columns.values());
      resources = resources.merge(sourceTable.readResources());
      return resources;
    }

    @Override
    public IterableTableReader reader(Object key, Resources resources, TableContext context) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(key, resources, context)) {

        @Override
        public Object[] next() throws IOException {
          Object[] sourceValues = sourceReader.next();
          if (sourceValues == null) {
            return null;
          }
          Object[] values = new Object[columns.size()];
          int i = 0;
          for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
            values[i++] = entry.getValue().value(sourceValues, resources, context);
          }
          return values;
        }
      };
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class FilteredTableMeta extends ExposedTableMeta {
    private final ExposedTableMeta sourceTable;
    private final ColumnMeta whereColumn;

    private Object[] values;

    @Override
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public ImmutableList<DataType<?>> columnTypes() {
      return sourceTable.columnTypes();
    }

    @Override
    public IndexColumn getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources().merge(resourcesFromColumn(whereColumn));
    }

    public IterableTableReader reader(Object key, Resources resources, TableContext context) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(key, resources, context)) {

        @Override
        public Object[] next() throws IOException {
          Object[] values = null;
          do {
            values = sourceReader.next();
            if (values == null) {
              return null;
            }
            if ((boolean) whereColumn.value(values, resources, context)) {
              return values;
            } else {
              values = null;
            }
          } while (values == null && !cancelled.get());
          return values;
        }
      };
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class IndexFilteredTableMeta extends ExposedTableMeta {
    private final VariableEquals clause;
    private final SeekableTableMeta sourceTable;
    private final ColumnMeta whereColumn;

    public IndexFilteredTableMeta(
        SeekableTableMeta sourceTable,
        ColumnMeta whereColumn,
        VariableEquals clause) {
      this.clause = clause;
      this.sourceTable = sourceTable;
      this.whereColumn = whereColumn;
    }

    @Override
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public ImmutableList<DataType<?>> columnTypes() {
      return sourceTable.columnTypes();
    }

    @Override
    public IndexColumn getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident);
    }

    @Override
    public TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources().merge(resourcesFromColumn(whereColumn));
    }

    @Override
    public IterableTableReader reader(Object key, Resources resources, TableContext context) throws IOException {
      return new MultiFilteredTableReader(resources.reader(sourceTable.tableName()), whereColumn, resources) {
        @Override
        protected void readPositions() throws IOException {
          IndexReader index = resources.getIndex(sourceTable.tableName(), clause.getIdent().getString());
          positions = index.get(clause.getValue());
        }
      };
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class KeyValueTableMeta extends IterableTableMeta {
    private final IterableTableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> keyColumns;

    @Override
    public IndexColumn getColumn(Ident ident) throws ModelException {
      if (keyColumns.containsKey(ident.getString())) {
        ColumnMeta column = keyColumns.get(ident.getString());
        return IndexColumn.of(this, column, indexOf(keyColumns.keySet(), ident));
      } else {
        return null;
      }
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return null;
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources().merge(resourcesFromColumns(keyColumns.values()));
    }

    @Override
    public IterableTableReader reader(Object key, Resources resources, TableContext context) throws IOException {
      return sourceTable.reader(key, resources, context);
    }

    @Override
    public ImmutableList<String> columnNames() {
      return keyColumns.keySet().asList();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class SortedTableMeta extends ExposedTableMeta {
    private final ExposedTableMeta sourceTable;
    private final ImmutableList<OrderColumn> orderColumns;

    private Object[] values;
    private boolean sorted;
    private Iterator<Object[]> iterator;

    @Override
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public ImmutableList<DataType<?>> columnTypes() {
      return sourceTable.columnTypes();
    }

    @Override
    public IndexColumn getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }

    public IterableTableReader reader(Object key, Resources resources, TableContext context) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(key, resources, context)) {

        private void sort() throws IOException {
          TreeMap<ArrayList<Object>, Object[]> values = new TreeMap<>(new Comparator<ArrayList<Object>>() {
            @Override
            public int compare(ArrayList<Object> x, ArrayList<Object> y) {
              for (int i = 0; i < orderColumns.size(); i++) {
                if (x.get(i) == null) {
                  if (y.get(i) == null) {
                    return 0;
                  } else {
                    return -1;
                  }
                } else if (y.get(i) == null) {
                  return 1;
                }
                int result = orderColumns.get(i).compare(x.get(i), y.get(i));
                if (result != 0) {
                  return result;
                }
              }
              return 0;
            }
          });
          while (!cancelled.get()) {
            Object[] sourceValues = sourceReader.next();
            if (sourceValues == null) {
              break;
            }
            ArrayList<Object> list = new ArrayList<>();
            for (OrderColumn column : orderColumns) {
              Object key = column.value(sourceValues, resources, context);
              list.add(key);
            }
            values.put(list, sourceValues);
          }
          iterator = values.values().iterator();
          sorted = true;
        }

        @Override
        public Object[] next() throws IOException {
          if (!sorted) {
            sort();
          }
          if (!iterator.hasNext()) {
            return null;
          }
          return iterator.next();
        }
      };
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DistinctTableMeta extends ExposedTableMeta {
    private final ExposedTableMeta sourceTable;

    private Object[] values;
    private boolean distinct;
    private Iterator<Object[]> iterator;

    @Override
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public ImmutableList<DataType<?>> columnTypes() {
      return sourceTable.columnTypes();
    }

    @Override
    public IndexColumn getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }

    @Override
    public IterableTableReader reader(Object key, Resources resources, TableContext context) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(key, resources, context)) {

        private void distinct() throws IOException {
          LinkedHashSet<ImmutableList<Object>> values = new LinkedHashSet<>();
          while (!cancelled.get()) {
            Object[] sourceValues = sourceReader.next();
            if (sourceValues == null) {
              break;
            }
            values.add(ImmutableList.copyOf(sourceValues));
          }
          iterator = values.stream().map(list -> list.toArray()).iterator();
          distinct = true;
        }

        @Override
        public Object[] next() throws IOException {
          if (!distinct) {
            distinct();
          }
          if (!iterator.hasNext()) {
            return null;
          }
          return iterator.next();
        }
      };
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AliasedTableMeta extends ExposedTableMeta {
    private final Ident ident;
    private final ExposedTableMeta sourceTable;

    public AliasedTableMeta(Ident ident, ExposedTableMeta sourceTable) {
      this.ident = ident;
      this.sourceTable = sourceTable;
    }

    @Override
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public ImmutableList<DataType<?>> columnTypes() {
      return sourceTable.columnTypes();
    }

    @Override
    public IndexColumn getColumn(Ident ident) throws ModelException {
      return sourceTable.getColumn(ident);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      if (this.ident.getString().equals(ident.getString())) {
        return sourceTable;
      }
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }

    @Override
    public IterableTableReader reader(Object key, Resources resources, TableContext context) throws IOException {
      return sourceTable.reader(key, resources, context);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ShiftedTableMeta extends IterableTableMeta {
    private final IterableTableMeta sourceTable;
    private final TableMeta childTable;
    private final int shift;

    @Override
    public IndexColumn getColumn(Ident ident) throws ModelException {
      return childTable.getColumn(ident).shift(sourceTable, shift);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }

    @Override
    public IterableTableReader reader(Object key, Resources resources, TableContext context) throws IOException {
      return sourceTable.reader(key, resources, context);
    }

    @Override
    public ImmutableList<String> columnNames() {
      return childTable.columnNames();
    }
  }
}
