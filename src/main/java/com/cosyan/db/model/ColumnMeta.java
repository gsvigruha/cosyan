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

import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.model.BuiltinFunctions.TypedAggrFunction;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.References.ReferencedTable;
import com.cosyan.db.model.TableMeta.IterableTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;

@Data
public abstract class ColumnMeta implements CompiledObject {

  protected final DataType<?> type;

  public abstract Object value(Object[] values, Resources resources, TableContext context) throws IOException;

  public abstract String print(Object[] values, Resources resources, TableContext context) throws IOException;

  public abstract TableDependencies tableDependencies();

  public abstract MetaResources readResources();

  public static class IndexColumn extends ColumnMeta {
    private final MetaResources metaResources;
    private final int index;
    private final TableDependencies tableDependencies;

    public static IndexColumn of(IterableTableMeta sourceTable, ColumnMeta parentColumn, int index) {
      return new IndexColumn(sourceTable, index, parentColumn.getType(), parentColumn.tableDependencies());
    }

    protected IndexColumn(MetaResources metaResources, int index, DataType<?> type, TableDependencies tableDependencies) {
      super(type);
      this.metaResources = metaResources;
      this.index = index;
      this.tableDependencies = tableDependencies;
    }

    public IndexColumn(IterableTableMeta sourceTable, int index, DataType<?> type, TableDependencies tableDependencies) {
      super(type);
      this.metaResources = sourceTable.readResources();
      this.index = index;
      this.tableDependencies = tableDependencies;
    }

    @Override
    public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
      return values[index];
    }

    @Override
    public String print(Object[] values, Resources resources, TableContext context) throws IOException {
      return String.valueOf(values[index]);
    }

    @Override
    public MetaResources readResources() {
      return metaResources;
    }

    @Override
    public TableDependencies tableDependencies() {
      return tableDependencies;
    }

    public int index() {
      return index;
    }

    public IndexColumn shift(IterableTableMeta sourceTable, int shift) {
      return new IndexColumn(sourceTable, index + shift, type, tableDependencies);
    }
  }

  public static class ReferencedIndexColumn extends IndexColumn {
    private final ReferencedTable referencedTable;
    private final int index;

    public static ReferencedIndexColumn of(ReferencedTable sourceTable, ColumnMeta parentColumn, int index) {
      return new ReferencedIndexColumn(sourceTable, index, parentColumn.getType(), parentColumn.tableDependencies());
    }

    public ReferencedIndexColumn(ReferencedTable referencedTable, int index, DataType<?> type, TableDependencies tableDependencies) {
      super(referencedTable.readResources(), index, type, tableDependencies);
      this.referencedTable = referencedTable;
      this.index = index;
    }

    @Override
    public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
      return referencedTable.values(values, resources, context)[index];
    }

    @Override
    public String print(Object[] values, Resources resources, TableContext context) throws IOException {
      return String.valueOf(referencedTable.values(values, resources, context)[index]);
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

    public DerivedColumnWithDeps(DataType<?> type, TableDependencies deps, MetaResources readResources) {
      super(type);
      this.deps = deps;
      this.readResources = readResources;
    }

    @Override
    public TableDependencies tableDependencies() {
      return deps;
    }

    @Override
    public MetaResources readResources() {
      return readResources;
    }
  }

  public static class AggrColumn extends DerivedColumn {

    private final AggrTables aggrTable;
    private final int index;
    private final ColumnMeta baseColumn;
    private final TypedAggrFunction<?> function;

    public AggrColumn(AggrTables aggrTable, DataType<?> type, ColumnMeta baseColumn, int index,
        TypedAggrFunction<?> function) {
      super(type);
      this.aggrTable = aggrTable;
      this.baseColumn = baseColumn;
      this.index = index;
      this.function = function;
    }

    @Override
    public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
      return values[index];
    }

    @Override
    public String print(Object[] values, Resources resources, TableContext context) throws IOException {
      return String.valueOf(values[index]);
    }

    public Object getInnerValue(Object[] values, Resources resources, TableContext context) throws IOException {
      return baseColumn.value(values, resources, context);
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
      TableDependencies deps = new TableDependencies();
      deps.addToThis(baseColumn.tableDependencies());
      deps.addToThis(aggrTable.tableDependencies());
      return deps;
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
    public Object value(Object[] values, Resources resources, TableContext context) throws IOException {
      return baseColumn.value(values, resources, context);
    }

    @Override
    public String print(Object[] values, Resources resources, TableContext context) throws IOException {
      return "order by " + baseColumn.print(values, resources, context) + (asc ? " asc" : " desc");
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
  }

  public static final DerivedColumn TRUE_COLUMN = new DerivedColumn(DataTypes.BoolType) {

    @Override
    public Object value(Object[] values, Resources resources, TableContext context) {
      return true;
    }

    @Override
    public String print(Object[] values, Resources resources, TableContext context) throws IOException {
      return "true";
    }

    @Override
    public TableDependencies tableDependencies() {
      return new TableDependencies();
    }

    @Override
    public MetaResources readResources() {
      return MetaResources.empty();
    }
  };
}
