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

import com.cosyan.db.io.RecordProvider.Record;
import com.cosyan.db.io.TableReader.DerivedIterableTableReader;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.TableProvider;
import com.cosyan.db.model.ColumnMeta.IndexColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.References.ReferencedMultiTableMeta;
import com.cosyan.db.model.References.ReferencedSimpleTableMeta;
import com.cosyan.db.model.References.ReferencedTable;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;

public class SeekableTableMeta extends ExposedTableMeta implements ReferencedTable, TableProvider {

  private final MaterializedTable tableMeta;

  public SeekableTableMeta(MaterializedTable tableMeta) {
    this.tableMeta = tableMeta;
  }

  public Record get(Resources resources, long position) throws IOException {
    return resources.reader(tableName()).get(position);
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
  public IndexColumn getColumn(Ident ident) throws ModelException {
    BasicColumn column = tableMeta.column(ident);
    if (column == null) {
      return null;
    }
    int index = tableMeta.columnNames().indexOf(column.getName());
    return new IndexColumn(this, index, column.getType(), new TableDependencies());
  }

  @Override
  public TableMeta getRefTable(Ident ident) throws ModelException {
    return References.getRefTable(
        this,
        tableMeta.tableName(),
        ident,
        tableMeta.foreignKeys(),
        tableMeta.reverseForeignKeys(),
        tableMeta.refs());
  }

  @Override
  public MetaResources readResources() {
    return MetaResources.readTable(tableMeta);
  }

  public String tableName() {
    return tableMeta.tableName();
  }

  public MaterializedTable tableMeta() {
    return tableMeta;
  }

  @Override
  public ImmutableList<Ref> foreignKeyChain() {
    return ImmutableList.of();
  }

  @Override
  public TableMeta tableMeta(Ident ident) throws ModelException {
    if (tableName().equals(ident.getString())) {
      return this;
    } else if (tableMeta.hasReverseForeignKey(ident.getString())) {
      return new ReferencedMultiTableMeta(this, tableMeta.reverseForeignKey(ident));
    } else {
      throw new ModelException(String.format("Table '%s' not found.", ident.getString()), ident);
    }
  }

  @Override
  public TableProvider tableProvider(Ident ident) throws ModelException {
    if (tableMeta.hasForeignKey(ident.getString())) {
      return new ReferencedSimpleTableMeta(this, tableMeta.foreignKey(ident));
    } else {
      throw new ModelException(String.format("Table '%s' not found.", ident.getString()), ident);
    }
  }

  @Override
  public IterableTableReader reader(Resources resources, TableContext context) throws IOException {
    return new DerivedIterableTableReader(resources.createIterableReader(tableName())) {

      @Override
      public Object[] next() throws IOException {
        return sourceReader.next();
      }
    };
  }

  @Override
  public TableMeta parent() {
    return this;
  }

  @Override
  public TableDependencies tableDependencies() {
    return new TableDependencies();
  }
}
