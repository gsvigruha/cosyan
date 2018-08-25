/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import com.cosyan.db.meta.Dependencies.TableDependencies;
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
import com.google.common.collect.Lists;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class DerivedTables {

  public static MetaResources resourcesFromColumns(Iterable<? extends ColumnMeta> columns) {
    MetaResources resources = MetaResources.empty();
    for (ColumnMeta columnMeta : columns) {
      resources = resources.merge(columnMeta.readResources());
    }
    return resources;
  }

  private static MetaResources resourcesFromColumn(ColumnMeta column) {
    return MaterializedTable.readResources(column.tableDependencies().getDeps().values());
  }

  /**
   * Table with a new set of derived column expressions.
   */
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
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(resources, context)) {

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

    @Override
    public TableDependencies tableDependencies() {
      return sourceTable.tableDependencies();
    }
  }

  /**
   * Table with a filter expression.
   */
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

    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(resources, context)) {

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

    @Override
    public TableDependencies tableDependencies() {
      return sourceTable.tableDependencies();
    }
  }

  /**
   * Table with a filter expression which is used in an index.
   */
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class IndexFilteredTableMeta extends ExposedTableMeta {
    private final SeekableTableMeta sourceTable;
    private final ColumnMeta whereColumn;
    private final VariableEquals clause;

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
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      return new MultiFilteredTableReader(resources.reader(sourceTable.fullName()), whereColumn, resources) {
        @Override
        protected void readPositions() throws IOException {
          IndexReader index = resources.getIndex(sourceTable.fullName(), clause.getIdent().getString());
          positions = index.get(clause.getValue());
        }
      };
    }

    @Override
    public TableDependencies tableDependencies() {
      return sourceTable.tableDependencies();
    }
  }

  /**
   * A table with key expressions to aggregate on.
   */
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
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      return sourceTable.reader(resources, context);
    }

    @Override
    public ImmutableList<String> columnNames() {
      return keyColumns.keySet().asList();
    }

    @Override
    public TableDependencies tableDependencies() {
      return sourceTable.tableDependencies();
    }
  }

  /**
   * A sorted table.
   */
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class SortedTableMeta extends ExposedTableMeta {
    private final ExposedTableMeta sourceTable;
    private final ImmutableList<OrderColumn> orderColumns;

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

    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(resources, context)) {

        private boolean sorted;
        private Iterator<Object[]> iterator;

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

    @Override
    public TableDependencies tableDependencies() {
      return sourceTable.tableDependencies();
    }
  }

  /**
   * A table containing only distinct rows.
   */
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DistinctTableMeta extends ExposedTableMeta {
    private final ExposedTableMeta sourceTable;

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
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      return new DerivedIterableTableReader(sourceTable.reader(resources, context)) {

        private boolean distinct;
        private Iterator<Object[]> iterator;

        private void distinct() throws IOException {
          LinkedHashSet<ArrayList<Object>> values = new LinkedHashSet<>();
          while (!cancelled.get()) {
            Object[] sourceValues = sourceReader.next();
            if (sourceValues == null) {
              break;
            }
            values.add(Lists.newArrayList(sourceValues));
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

    @Override
    public TableDependencies tableDependencies() {
      return sourceTable.tableDependencies();
    }
  }

  /**
   * An aliased table.
   */
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
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      return sourceTable.reader(resources, context);
    }

    @Override
    public TableDependencies tableDependencies() {
      return sourceTable.tableDependencies();
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
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      return sourceTable.reader(resources, context);
    }

    @Override
    public ImmutableList<String> columnNames() {
      return childTable.columnNames();
    }

    @Override
    public TableDependencies tableDependencies() {
      return sourceTable.tableDependencies();
    }
  }

  /**
   * A table with a limit on the number of returned rows.
   */
  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class LimitedTableMeta extends ExposedTableMeta {
    private final ExposedTableMeta sourceTable;
    private final long limit;

    @Override
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    public ImmutableList<DataType<?>> columnTypes() {
      return sourceTable.columnTypes();
    }

    @Override
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      final IterableTableReader reader = sourceTable.reader(resources, context);
      return new IterableTableReader() {
        private int i = 0;

        @Override
        public Object[] next() throws IOException {
          if (i >= limit) {
            return null;
          }
          i++;
          return reader.next();
        }

        @Override
        public void close() throws IOException {
          reader.close();
        }
      };
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
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
    public TableDependencies tableDependencies() {
      return sourceTable.tableDependencies();
    }
  }
}
