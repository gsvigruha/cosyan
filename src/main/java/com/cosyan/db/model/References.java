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
import com.cosyan.db.model.AggrTables.GlobalAggrTableMeta;
import com.cosyan.db.model.AggrTables.KeyValueAggrTableMeta;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
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
      return refTable.values(parent.values(sourceValues, resources, context), resources, context);
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      IndexColumn column = refTable.column(ident);
      if (column == null) {
        return null;
      }
      TableDependencies deps = new TableDependencies(this, column.tableDependencies());
      return new IndexColumn(this, column.index(), column.getType(), deps);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return refTable.getRefTable(ident);
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
      BasicColumn column = foreignKey.getRefTable().column(ident);
      if (column == null) {
        return null;
      }
      TableDependencies deps = new TableDependencies();
      deps.addTableDependency(this);
      return new IndexColumn(this, column.getIndex(), column.getType(), deps);
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return References.getRefTable(this, foreignKey.getRefTable().tableName(), ident,
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
      return ImmutableList.<Ref>builder().addAll(parent.foreignKeyChain()).add(reverseForeignKey)
          .build();
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      BasicColumn column = reverseForeignKey.getRefTable().column(ident);
      if (column == null) {
        return null;
      }
      TableDependencies deps = new TableDependencies();
      deps.addTableDependency(this);
      return new IndexColumn(this, column.getIndex(), column.getType(), deps);
    }

    @Override
    public TableDependencies tableDependencies() {
      TableDependencies deps = new TableDependencies();
      deps.addTableDependency(this);
      return deps;
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      if (ident.is(Tokens.PARENT)) {
        return new ParentTableMeta(foreignKeyChain().get(0).getTable().reader());
      }
      return References.getRefTable(this, reverseForeignKey.getTable().tableName(), ident,
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
        protected void readPositions() throws IOException {
          positions = index.get(key);
        }
      };
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
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class GroupByFilterTableMeta extends ExposedTableMeta implements ReferencedTable {

    private final SeekableTableMeta tableMeta;
    private final GroupByKey groupByKey;

    public GroupByFilterTableMeta(SeekableTableMeta tableMeta, GroupByKey groupByKey) {
      this.tableMeta = tableMeta;
      this.groupByKey = groupByKey;
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      BasicColumn column = tableMeta.tableMeta().column(ident);
      if (column == null) {
        return null;
      }
      TableDependencies deps = new TableDependencies();
      deps.addTableDependency(this);
      return new IndexColumn(this, column.getIndex(), column.getType(), deps);
    }

    @Override
    public TableDependencies tableDependencies() {
      TableDependencies deps = new TableDependencies();
      deps.addTableDependency(this);
      return deps;
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
      return tableMeta.getRefTable(ident);
    }

    @Override
    public MetaResources readResources() {
      return tableMeta.readResources();
    }

    @Override
    public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
      Object[] sourceValues = context.values(TableContext.PARENT);
      Object[] key = groupByKey.resolveKey(sourceValues, resources, context);
      final IndexReader index = resources.getIndex(groupByKey);
      return new MultiFilteredTableReader(resources.reader(tableMeta.fullName()),
          ColumnMeta.TRUE_COLUMN, resources) {

        @Override
        protected void readPositions() throws IOException {
          positions = index.get(key);
        }
      };
    }

    @Override
    public ImmutableList<String> columnNames() {
      return tableMeta.columnNames();
    }

    @Override
    public ImmutableList<DataType<?>> columnTypes() {
      return tableMeta.columnTypes();
    }

    @Override
    public ImmutableList<Ref> foreignKeyChain() {
      return ImmutableList.<Ref>builder().addAll(tableMeta.foreignKeyChain()).add(groupByKey)
          .build();
    }

    @Override
    public TableMeta parent() {
      return tableMeta.parent();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AggRefTableMeta extends TableMeta {
    private final GlobalAggrTableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> columns;

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      ColumnMeta column = columns.get(ident.getString());
      if (column == null) {
        return null;
      }
      return new IndexColumn(sourceTable, indexOf(columns.keySet(), ident), column.getType(),
          column.tableDependencies());
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
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

    @Override
    public Object[] values(Object[] sourceValues, Resources resources, TableContext context)
        throws IOException {
      IterableTableReader reader = sourceTable.reader(resources, TableContext.withParent(sourceValues));
      Object[] aggrValues = reader.next();
      reader.close();
      Object[] values = new Object[columns.size()];
      int i = 0;
      for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
        values[i++] = entry.getValue().value(aggrValues, resources, context);
      }
      return values;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class FlatRefTableMeta extends TableMeta {
    private final ExposedTableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> columns;

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      ColumnMeta column = columns.get(ident.getString());
      if (column == null) {
        return null;
      }
      return new IndexColumn(sourceTable, indexOf(columns.keySet(), ident), column.getType(),
          column.tableDependencies());
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
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

    @Override
    public Object[] values(Object[] sourceValues, Resources resources, TableContext context)
        throws IOException {
      Object[] values = sourceTable.values(sourceValues, resources, context);
      Object[] newValues = new Object[columns.size()];
      int i = 0;
      for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
        newValues[i++] = entry.getValue().value(values, resources, context);
      }
      return newValues;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class AggViewTableMeta extends TableMeta {
    private final KeyValueAggrTableMeta sourceTable;
    private final ImmutableMap<String, ColumnMeta> columns;

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      ColumnMeta column = columns.get(ident.getString());
      if (column == null) {
        return null;
      }
      return new IndexColumn(sourceTable, indexOf(columns.keySet(), ident), column.getType(),
          column.tableDependencies());
    }

    @Override
    protected TableMeta getRefTable(Ident ident) throws ModelException {
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

    @Override
    public Object[] values(Object[] sourceValues, Resources resources, TableContext context)
        throws IOException {
      IterableTableReader reader = sourceTable.reader(resources, TableContext.withParent(sourceValues));
      Object[] aggrValues = reader.next();
      reader.close();
      Object[] values = new Object[columns.size()];
      int i = 0;
      for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
        values[i++] = entry.getValue().value(aggrValues, resources, context);
      }
      return values;
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
    public Object[] values(Object[] sourceValues, Resources resources, TableContext context)
        throws IOException {
      return context.values(TableContext.PARENT);
    }

    @Override
    protected IndexColumn getColumn(Ident ident) throws ModelException {
      IndexColumn column = sourceTable.column(ident);
      if (column == null) {
        return null;
      }
      return IndexColumn.of(this, column, column.index());
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
