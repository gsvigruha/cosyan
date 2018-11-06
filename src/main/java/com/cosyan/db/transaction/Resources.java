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
package com.cosyan.db.transaction;

import java.io.IOException;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.Indexes.IndexWriter;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.meta.DBObject;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.GroupByKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.TableUniqueIndex;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class Resources {

  private final ImmutableMap<String, SeekableTableReader> readers;
  private final ImmutableMap<String, TableWriter> writers;
  private final ImmutableMap<String, DBObject> metas;

  public Resources(
      ImmutableMap<String, SeekableTableReader> readers,
      ImmutableMap<String, TableWriter> writers,
      ImmutableMap<String, DBObject> metas) {
    assert Sets.intersection(readers.keySet(), writers.keySet()).isEmpty();
    this.readers = readers;
    this.writers = writers;
    this.metas = metas;
  }

  public void rollback() {
    for (TableWriter table : writers.values()) {
      table.rollback();
    }
  }

  public void commit() throws IOException {
    for (TableWriter table : writers.values()) {
      table.commit();
    }
  }

  public TableWriter writer(String table) {
    return Preconditions.checkNotNull(writers.get(table));
  }

  public DBObject meta(String table) {
    assert metas.containsKey(table) : String.format("Invalid table %s.", table);
    return Preconditions.checkNotNull(metas.get(table));
  }

  public SeekableTableReader reader(String table) {
    assert readers.containsKey(table) || writers.containsKey(table) : String.format("Invalid table %s.", table);
    if (readers.containsKey(table)) {
      return Preconditions.checkNotNull(readers.get(table));
    } else {
      return Preconditions.checkNotNull(writers.get(table));
    }
  }

  public IterableTableReader createIterableReader(String table) throws IOException {
    return Preconditions.checkNotNull(reader(table).iterableReader(this));
  }

  public TableUniqueIndex getPrimaryKeyIndex(String table) {
    return Preconditions.checkNotNull(reader(table).getPrimaryKeyIndex());
  }

  public IndexReader getIndex(String table, String column) {
    return Preconditions.checkNotNull(reader(table).getIndex(column));
  }

  public IndexWriter indexWriter(String table, String column) {
    assert writers.containsKey(table) : String.format("Invalid table %s.", table);
    return writers.get(table).getIndexWriter(column);
  }

  public IndexReader getIndex(ForeignKey foreignKey) {
    return Preconditions.checkNotNull(getIndex(foreignKey.getRefTable().fullName(), foreignKey.getRefColumn().getName()));
  }

  public IndexReader getIndex(ReverseForeignKey foreignKey) {
    return Preconditions.checkNotNull(getIndex(foreignKey.getRefTable().fullName(), foreignKey.getRefColumn().getName()));
  }

  public IndexReader getIndex(GroupByKey groupByKey) {
    return Preconditions.checkNotNull(reader(groupByKey.getRefTable().fullName()).getIndex(groupByKey.getName()));
  }
}
