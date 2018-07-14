package com.cosyan.db.transaction;

import java.io.IOException;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.Indexes.IndexWriter;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.TableUniqueIndex;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class Resources {

  private final ImmutableMap<String, SeekableTableReader> readers;
  private final ImmutableMap<String, TableWriter> writers;
  private final ImmutableMap<String, MaterializedTable> metas;

  public Resources(
      ImmutableMap<String, SeekableTableReader> readers,
      ImmutableMap<String, TableWriter> writers,
      ImmutableMap<String, MaterializedTable> metas) {
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
    return writers.get(table);
  }

  public MaterializedTable meta(String table) {
    assert metas.containsKey(table);
    return metas.get(table);
  }

  public SeekableTableReader reader(String table) throws IOException {
    assert readers.containsKey(table) || writers.containsKey(table) : String.format("Invalid table %s.", table);
    if (readers.containsKey(table)) {
      return readers.get(table);
    } else {
      return writers.get(table);
    }
  }

  public IterableTableReader createIterableReader(String table) throws IOException {
    assert readers.containsKey(table) || writers.containsKey(table) : String.format("Invalid table %s.", table);
    if (readers.containsKey(table)) {
      return readers.get(table).iterableReader();
    } else {
      return writers.get(table).iterableReader();
    }
  }

  public TableUniqueIndex getPrimaryKeyIndex(String table) {
    assert readers.containsKey(table) || writers.containsKey(table) : String.format("Invalid table %s.", table);
    if (readers.containsKey(table)) {
      return readers.get(table).getPrimaryKeyIndex();
    } else {
      return writers.get(table).getPrimaryKeyIndex();
    }
  }

  public IndexReader getIndex(String table, String column) {
    assert readers.containsKey(table) || writers.containsKey(table) : String.format("Invalid table %s.", table);
    if (readers.containsKey(table)) {
      return readers.get(table).getIndex(column);
    } else {
      return writers.get(table).getIndex(column);
    }
  }

  public IndexWriter indexWriter(String table, String column) {
    assert writers.containsKey(table) : String.format("Invalid table %s.", table);
    return writers.get(table).getIndexWriter(column);
  }

  public IndexReader getIndex(Ref foreignKey) {
    return getIndex(foreignKey.getRefTable().tableName(), foreignKey.getRefColumn().getName());
  }
}
