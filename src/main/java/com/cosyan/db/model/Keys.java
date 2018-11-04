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

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.meta.DBObject;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.View;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;

public class Keys {

  @Data
  public static class PrimaryKey {
    private final Ident name;
    private final BasicColumn column;
  }

  public static interface Ref {

    String getName();

    MaterializedTable getTable();

    DBObject getRefTable();

    Ref getReverse();

    public long[] resolve(Object[] values, Resources resources) throws IOException;
  }

  @Data
  public static class ForeignKey implements Ref {
    private final String name;
    private final String revName;
    private final MaterializedTable table;
    private final BasicColumn column;
    private final MaterializedTable refTable;
    private final BasicColumn refColumn;

    @Override
    public String toString() {
      return name + " [" + column.getName() + " -> " + refTable.fullName() + "." + refColumn.getName() + "]";
    }

    public ReverseForeignKey createReverse() {
      return new ReverseForeignKey(revName, name, refTable, refColumn, table, column);
    }

    public ReverseForeignKey getReverse() {
      return refTable.reverseForeignKeys().get(revName);
    }

    @Override
    public long[] resolve(Object[] values, Resources resources) throws IOException {
      Object key = values[getColumn().getIndex()];
      IndexReader index = resources.getIndex(this);
      return index.get(key);
    }
  }

  @Data
  public static class ReverseForeignKey implements Ref {
    private final String name;
    private final String revName;
    private final MaterializedTable table;
    private final BasicColumn column;
    private final MaterializedTable refTable;
    private final BasicColumn refColumn;

    @Override
    public String toString() {
      return name + " [" + refTable.fullName() + "." + refColumn.getName() + " -> " + column.getName() + "]";
    }

    public ForeignKey getReverse() {
      return refTable.foreignKeys().get(revName);
    }

    @Override
    public long[] resolve(Object[] values, Resources resources) throws IOException {
      Object key = values[getColumn().getIndex()];
      IndexReader index = resources.getIndex(this);
      return index.get(key);
    }
  }

  @Data
  public static class GroupByKey implements Ref {
    private final String name;
    private final MaterializedTable table;
    private final View refView;
    private final ImmutableMap<String, ColumnMeta> columns;

    public ImmutableList<DataType<?>> columnTypes() {
      return getColumns().values().stream().map(c -> c.getType()).collect(ImmutableList.toImmutableList());
    }

    public Object[] resolveKey(Object[] values, Resources resources, TableContext context) throws IOException {
      ImmutableList<ColumnMeta> columnList = columns.values().asList();
      Object[] key = new Object[columns.size()];
      for (int i = 0; i < key.length; i++) {
        key[i] = columnList.get(i).value(values, resources, context);
      }
      return key;
    }

    @Override
    public long[] resolve(Object[] values, Resources resources) throws IOException {
      Object[] key = resolveKey(values, resources, TableContext.EMPTY);
      IndexReader index = resources.getIndex(this);
      return index.get(key);
    }

    @Override
    public DBObject getRefTable() {
      return refView.dbObject();
    }

    @Override
    public Ref getReverse() {
      return this;
    }
  }
}
