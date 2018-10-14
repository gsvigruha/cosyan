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

import javax.annotation.Nullable;

import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public abstract class TableMeta implements CompiledObject {

  public static final ImmutableMap<String, ColumnMeta> wholeTableKeys = ImmutableMap.of("",
      ColumnMeta.TRUE_COLUMN);

  public IndexColumn column(Ident ident) throws ModelException {
    IndexColumn column = getColumn(ident);
    if (column == null) {
      throw new ModelException(String.format("Column '%s' not found in table.", ident), ident);
    }
    return column;
  }

  public boolean hasColumn(Ident ident) {
    try {
      return getColumn(ident) != null;
    } catch (ModelException e) {
      return false;
    }
  }

  public abstract ImmutableList<String> columnNames();

  public abstract Object[] values(Object[] sourceValues, Resources resources, TableContext context) throws IOException;

  public TableMeta table(Ident ident) throws ModelException {
    TableMeta table = getRefTable(ident);
    if (table == null) {
      throw new ModelException(String.format("Table reference '%s' not found.", ident), ident);
    }
    return table;
  }

  public boolean hasTable(Ident ident) {
    try {
      return getRefTable(ident) != null;
    } catch (ModelException e) {
      return false;
    }
  }

  @Nullable
  protected abstract IndexColumn getColumn(Ident ident) throws ModelException;

  @Nullable
  protected abstract TableMeta getRefTable(Ident ident) throws ModelException;

  public abstract MetaResources readResources();

  protected int indexOf(ImmutableSet<String> keys, Ident ident) {
    return keys.asList().indexOf(ident.getString());
  }

  protected IndexColumn shiftColumn(TableMeta sourceTable, ImmutableMap<String, ColumnMeta> columns, Ident ident)
      throws ModelException {
    ColumnMeta column = columns.get(ident.getString());
    if (column == null) {
      return null;
    }
    return new IndexColumn(sourceTable, indexOf(columns.keySet(), ident), column.getType(), column.tableDependencies());
  }

  public static abstract class IterableTableMeta extends TableMeta {

    public abstract IterableTableReader reader(Resources resources, TableContext context) throws IOException;

    public abstract TableDependencies tableDependencies();

    // Iterable tables cannot override this function.
    public final Object[] values(Object[] sourceValues, Resources resources, TableContext context) throws IOException {
      return sourceValues;
    }
  }

  public static abstract class ExposedTableMeta extends IterableTableMeta {
    public abstract ImmutableList<String> columnNames();

    public abstract ImmutableList<DataType<?>> columnTypes();
  }
}
