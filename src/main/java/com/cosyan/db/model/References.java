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
import java.util.Arrays;
import java.util.Map;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableReader.MultiFilteredTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.lang.sql.Tokens;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.TableProvider;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
import com.cosyan.db.model.ColumnMeta.ReferencedIndexColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.GroupByKey;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class References {

  /**
   * A table referenced form another table through a chain of foreign keys or
   * reverse foreign keys. A chain of references uniquely identifies a
   * ReferencedTable.
   * 
   * @author gsvigruha
   */
  public static interface ReferencedTable {

    /**
     * The chain of references (foreign keys or reverse foreign keys) leading to
     * this table.
     */
    public ImmutableList<Ref> foreignKeyChain();

    /**
     * Returns all transitive read resources needed to for this table.
     */
    public MetaResources readResources();

    /**
     * Returns the referenced values based on sourceValues.
     */
    public Object[] values(Object[] sourceValues, Resources resources, TableContext context)
        throws IOException;

    /**
     * Returns the immediate parent table referencing this table.
     */
    public TableMeta parent();
  }

  public static IndexColumn columnWithDep(BasicColumn column, ReferencedTable table) {
    if (column == null) {
      return null;
    }
    return new ReferencedIndexColumn(table, column.getIndex(), column.getType(), TableDependencies.of(table));
  }

  public static TableMeta getRefTable(ReferencedTable parent, String tableName, Ident key,
      Map<String, ForeignKey> foreignKeys, Map<String, ReverseForeignKey> reverseForeignKeys,
      Map<String, TableRef> refs) throws ModelException {
    String name = key.getString();
    if (foreignKeys.containsKey(name)) {
      return new ReferencedSimpleTableMeta(parent, foreignKeys.get(name));
    } else if (refs.containsKey(name)) {
      return new ReferencedRefTableMeta(parent, refs.get(name).getTableMeta());
    }
    throw new ModelException(
        String.format("Reference '%s' not found in table '%s'.", key, tableName), key);
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ReferencedRefTableMeta extends TableMeta implements ReferencedTable {

    private final ReferencedTable parent;
    private final TableMeta refTable;

    @Override
    public ImmutableList<String> columnNames() {
      return refTable.columnNames();
    }

    @Override
    public Object[] values(Object[] sourceValues, Resources resources, TableContext context)
        throws IOException {
      return refTable.values(parent.values(sourceValues, resources, context), resources);
    }

    @Override
    public Object[] values(Object[] key, Resources resources) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      IndexColumn column = refTable.column(ident);
      if (column == null) {
        return null;
      }
      TableDependencies deps = new TableDependencies(this, column.tableDependencies());
      return new ReferencedIndexColumn(this, column.index(), column.getType(), deps);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return null;
    }

    @Override
    public MetaResources readResources() {
      return refTable.readResources();
    }

    @Override
    public ImmutableList<Ref> foreignKeyChain() {
      return parent.foreignKeyChain();
    }

    @Override
    public TableMeta parent() {
      return parent.parent();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ReferencedSimpleTableMeta extends TableMeta
      implements ReferencedTable, TableProvider {

    private final ReferencedTable parent;
    private final ForeignKey foreignKey;
    private final Object[] nulls;

    public ReferencedSimpleTableMeta(ReferencedTable parent, ForeignKey foreignKey) {
      this.parent = parent;
      this.foreignKey = foreignKey;
      nulls = new Object[foreignKey.getRefTable().columns().size()];
      Arrays.fill(nulls, null);
    }

    @Override
    public ImmutableList<String> columnNames() {
      return foreignKey.getRefTable().columnNames();
    }

    @Override
    public ImmutableList<Ref> foreignKeyChain() {
      return ImmutableList.<Ref>builder().addAll(parent.foreignKeyChain()).add(foreignKey).build();
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      return columnWithDep(foreignKey.getRefTable().column(ident), this);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return References.getRefTable(this, foreignKey.getRefTable().name(), ident,
          foreignKey.getRefTable().foreignKeys(), foreignKey.getRefTable().reverseForeignKeys(),
          foreignKey.getRefTable().refs());
    }

    @Override
    public MetaResources readResources() {
      return parent.readResources().merge(MetaResources.readTable(foreignKey.getRefTable()));
    }

    @Override
    public Object[] values(Object[] sourceValues, Resources resources, TableContext context)
        throws IOException {
      Object[] parentValues = parent.values(sourceValues, resources, context);
      Object key = parentValues[foreignKey.getColumn().getIndex()];
      if (key == null) {
        return nulls;
      } else {
        SeekableTableReader reader = resources.reader(foreignKey.getRefTable().fullName());
        return reader.get(key, resources).getValues();
      }
    }

    @Override
    public Object[] values(Object[] key, Resources resources) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ExposedTableMeta tableMeta(TableWithOwner table) throws ModelException {
      if (foreignKey.getRefTable().reverseForeignKeys().containsKey(table.getTable().getString())) {
        return new ReferencedMultiTableMeta(this,
            foreignKey.getRefTable().reverseForeignKey(table.getTable()));
      } else {
        throw new ModelException(String.format("Table '%s' not found.", table.getTable().getString()),
            table.getTable());
      }
    }

    @Override
    public TableProvider tableProvider(Ident ident, String owner) throws ModelException {
      if (foreignKey.getRefTable().foreignKeys().containsKey(ident.getString())) {
        return new ReferencedSimpleTableMeta(this, foreignKey.getRefTable().foreignKey(ident));
      } else {
        throw new ModelException(String.format("Table '%s' not found.", ident.getString()), ident);
      }
    }

    @Override
    public TableMeta parent() {
      return parent.parent();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ReferencedMultiTableMeta extends ExposedTableMeta implements ReferencedTable {

    private final ReferencedTable parent;
    private final ReverseForeignKey reverseForeignKey;
    private final MaterializedTable sourceTable;

    public ReferencedMultiTableMeta(ReferencedTable parent, ReverseForeignKey reverseForeignKey) {
      this.parent = parent;
      this.reverseForeignKey = reverseForeignKey;
      this.sourceTable = getReverseForeignKey().getRefTable();
    }

    @Override
    public ImmutableList<Ref> foreignKeyChain() {
      return ImmutableList.<Ref>builder().addAll(parent.foreignKeyChain()).add(reverseForeignKey).build();
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      return columnWithDep(reverseForeignKey.getRefTable().column(ident), this);
    }

    @Override
    public TableDependencies tableDependencies() {
      return TableDependencies.of(this);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      if (ident.is(Tokens.PARENT)) {
        return new ParentTableMeta(((MaterializedTable) foreignKeyChain().get(0).getTable()).meta());
      }
      return References.getRefTable(this, reverseForeignKey.getTable().name(), ident,
          reverseForeignKey.getRefTable().foreignKeys(),
          reverseForeignKey.getRefTable().reverseForeignKeys(),
          reverseForeignKey.getRefTable().refs());
    }

    @Override
    public MetaResources readResources() {
      return parent.readResources().merge(MetaResources.readTable(reverseForeignKey.getRefTable()));
    }

    @Override
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      Object[] parentValues = parent.values(context.values(TableContext.PARENT), resources, context);
      Object key = parentValues[reverseForeignKey.getColumn().getIndex()];
      final IndexReader index = resources.getIndex(reverseForeignKey);
      return new MultiFilteredTableReader(resources.reader(reverseForeignKey.getRefTable().fullName()),
          ColumnMeta.TRUE_COLUMN, resources) {

        @Override
        protected long[] readPositions() throws IOException {
          return index.get(key);
        }
      };
    }

    @Override
    public Object[] values(Object[] key, Resources resources) throws IOException {
      throw new UnsupportedOperationException();
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
    public TableMeta parent() {
      return parent.parent();
    }

    @Override
    public Object[] values(Object[] sourceValues, Resources resources, TableContext context) throws IOException {
      return sourceValues;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class GroupByFilterTableMeta extends ExposedTableMeta implements ReferencedTable {

    private final SeekableTableMeta parent;
    private final GroupByKey groupByKey;

    public GroupByFilterTableMeta(SeekableTableMeta parent, GroupByKey groupByKey) {
      this.parent = parent;
      this.groupByKey = groupByKey;
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      return columnWithDep(parent.tableMeta().column(ident), this);
    }

    @Override
    public TableDependencies tableDependencies() {
      return TableDependencies.of(this);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return References.getRefTable(this, groupByKey.getRefTable().name(), ident,
          groupByKey.getRefTable().foreignKeys(), groupByKey.getRefTable().reverseForeignKeys(),
          groupByKey.getRefTable().refs());
    }

    @Override
    public MetaResources readResources() {
      return parent.readResources();
    }

    @Override
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      if (context.has(TableContext.PARENT)) {
        Object[] sourceValues = context.values(TableContext.PARENT);
        Object[] key = groupByKey.getReverse().resolveKey(sourceValues, resources, context);
        final IndexReader index = resources.getIndex(groupByKey);
        return new MultiFilteredTableReader(resources.reader(parent.fullName()),
            ColumnMeta.TRUE_COLUMN, resources) {

          @Override
          protected long[] readPositions() throws IOException {
            return index.get(key);
          }
        };
      } else {
        return parent.reader(resources, context);
      }
    }

    @Override
    public Object[] values(Object[] key, Resources resources) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImmutableList<String> columnNames() {
      return parent.columnNames();
    }

    @Override
    public ImmutableList<DataType<?>> columnTypes() {
      return parent.columnTypes();
    }

    @Override
    public ImmutableList<Ref> foreignKeyChain() {
      return ImmutableList.<Ref>builder().addAll(parent.foreignKeyChain()).add(groupByKey).build();
    }

    @Override
    public TableMeta parent() {
      return parent.parent();
    }

    @Override
    public Object[] values(Object[] sourceValues, Resources resources, TableContext context) throws IOException {
      return sourceValues;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AggRefTableMeta extends TableMeta {
    private final IterableTableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> columns;

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      return shiftColumn(sourceTable, columns, ident);
    }

    @Override
    public TableMeta getRefTable(Ident ident) throws ModelException {
      // Cannot reference any further tables from a ref, only access its fields.
      return null;
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources().merge(DerivedTables.resourcesFromColumns(columns.values()));
    }

    @Override
    public ImmutableList<String> columnNames() {
      return columns.keySet().asList();
    }

    public Object[] values(Object[] sourceValues, Resources resources, TableContext context)
        throws IOException {
      IterableTableReader reader = sourceTable.reader(resources, TableContext.withParent(sourceValues));
      Object[] aggrValues = reader.next();
      reader.close();
      return mapValues(aggrValues, resources, context, columns);
    }

    @Override
    public Object[] values(Object[] key, Resources resources) throws IOException {
      return values(key, resources, TableContext.EMPTY);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class ParentTableMeta extends TableMeta {

    private final SeekableTableMeta sourceTable;

    @Override
    public ImmutableList<String> columnNames() {
      return sourceTable.columnNames();
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      IndexColumn column = sourceTable.column(ident);
      if (column == null) {
        return null;
      }
      return new IndexColumn(sourceTable.readResources(), column.index(), column.getType(),
          column.tableDependencies()) {

        @Override
        public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
          return context.values(TableContext.PARENT)[index()];
        }

        @Override
        public String print(Object[] values, Resources resources, TableContext context) throws IOException {
          return String.valueOf(context.values(TableContext.PARENT)[index()]);
        }
      };
    }

    @Override
    public Object[] values(Object[] key, Resources resources) throws IOException {
      return sourceTable.values(key, resources);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return sourceTable.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return sourceTable.readResources();
    }
  }
}
