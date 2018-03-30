package com.cosyan.db.model;

import java.io.IOException;
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
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.Dependencies.TableDependencies;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.MaterializedTableMeta.SeekableTableMeta;
import com.cosyan.db.model.References.ReferencedAggrTableMeta;
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
    return MaterializedTableMeta.readResources(column.tableDependencies().getDeps().values());
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
    public IndexColumn getColumn(Ident ident) throws ModelException {
      ColumnMeta column = columns.get(ident.getString());
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
    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(key, resources)) {

        @Override
        public Object[] next() throws IOException {
          Object[] sourceValues = sourceReader.next();
          if (sourceValues == null) {
            return null;
          }
          Object[] values = new Object[columns.size()];
          int i = 0;
          for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
            values[i++] = entry.getValue().value(sourceValues, resources);
          }
          return values;
        }
      };
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ReferencingDerivedTableMeta extends ExposedTableMeta {
    private final ReferencedAggrTableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> columns;
    private final ReverseForeignKey reverseForeignKey;

    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(key, resources)) {

        @Override
        public Object[] next() throws IOException {
          Object[] sourceValues = sourceReader.next();
          if (sourceValues == null) {
            return null;
          }
          Object[] values = new Object[columns.size()];
          int i = 0;
          for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
            values[i++] = entry.getValue().value(sourceValues, resources);
          }
          return values;
        }
      };
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      ColumnMeta column = columns.get(ident.getString());
      TableDependencies deps = new TableDependencies(this, column.tableDependencies());
      return new IndexColumn(sourceTable, indexOf(columns.keySet(), ident), column.getType(), deps);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return null;
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources().merge(MetaResources.readTable(reverseForeignKey.getRefTable()));
    }

    public ImmutableList<String> columnNames() {
      return columns.keySet().asList();
    }

    @Override
    public Object[] values(Object[] sourceValues, Resources resources) throws IOException {
      return sourceTable.values(sourceValues, resources);
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

    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(key, resources)) {

        @Override
        public Object[] next() throws IOException {
          Object[] values = null;
          do {
            values = sourceReader.next();
            if (values == null) {
              return null;
            }
            if ((boolean) whereColumn.value(values, resources)) {
              return values;
            } else {
              values = null;
            }
          } while (values == null && !cancelled);
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

    @Override
    public IterableTableReader reader(Object key, Resources resources) throws IOException {
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
    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return sourceTable.reader(key, resources);
    }

    @Override
    public Object[] values(Object[] sourceValues, Resources resources) throws IOException {
      return sourceTable.values(sourceValues, resources);
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

    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(key, resources)) {

        private void sort() throws IOException {
          TreeMap<ImmutableList<Object>, Object[]> values = new TreeMap<>(new Comparator<ImmutableList<Object>>() {
            @Override
            public int compare(ImmutableList<Object> x, ImmutableList<Object> y) {
              for (int i = 0; i < orderColumns.size(); i++) {
                int result = orderColumns.get(i).compare(x.get(i), y.get(i));
                if (result != 0) {
                  return result;
                }
              }
              return 0;
            }
          });
          while (!cancelled) {
            Object[] sourceValues = sourceReader.next();
            if (sourceValues == null) {
              break;
            }
            ImmutableList.Builder<Object> builder = ImmutableList.builder();
            for (OrderColumn column : orderColumns) {
              Object key = column.value(sourceValues, resources);
              builder.add(key);
            }
            values.put(builder.build(), sourceValues);
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
    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(key, resources)) {

        private void distinct() throws IOException {
          LinkedHashSet<ImmutableList<Object>> values = new LinkedHashSet<>();
          while (!cancelled) {
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
    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return sourceTable.reader(key, resources);
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
    public IterableTableReader reader(Object key, Resources resources) throws IOException {
      return sourceTable.reader(key, resources);
    }
  }
}
