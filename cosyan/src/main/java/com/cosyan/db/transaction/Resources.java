package com.cosyan.db.transaction;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Optional;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.MaterializedTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.Keys.Ref;
import com.cosyan.db.model.TableIndex;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class Resources {

  public static class ReaderFactory {

    private final String fileName;
    private final ImmutableList<BasicColumn> columns;
    private final ImmutableMap<String, IndexReader> indexReaders;
    private final Optional<PrimaryKey> primaryKey;

    public ReaderFactory(String fileName, ImmutableList<BasicColumn> columns,
        ImmutableMap<String, IndexReader> indexReaders, Optional<PrimaryKey> primaryKey) {
      this.fileName = fileName;
      this.columns = columns;
      this.indexReaders = indexReaders;
      this.primaryKey = primaryKey;
    }

    public SeekableTableReader create(Resources resources) throws IOException {
      return new MaterializedTableReader(
          new RandomAccessFile(fileName, "rw"),
          columns,
          indexReaders,
          resources);
    }

    public TableIndex getPrimaryKeyIndex() {
      return (TableIndex) indexReaders.get(primaryKey.get().getColumn().getName());
    }
    
    public IndexReader getIndex(BasicColumn basicColumn) {
      return indexReaders.get(basicColumn.getName());
    }
  }

  private final ImmutableMap<String, ReaderFactory> readers;
  private final ImmutableMap<String, TableWriter> writers;

  public Resources(
      ImmutableMap<String, ReaderFactory> readers,
      ImmutableMap<String, TableWriter> writers) {
    assert Sets.intersection(readers.keySet(), writers.keySet()).isEmpty();
    this.readers = readers;
    this.writers = writers;
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

  public TableWriter writer(Ident table) {
    return writers.get(table.getString());
  }

  public SeekableTableReader createReader(String table) throws IOException {
    if (readers.containsKey(table)) {
      return readers.get(table).create(this);
    } else {
      return writers.get(table).createReader(this);
    }
  }

  public TableIndex getPrimaryKeyIndex(String table) {
    if (readers.containsKey(table)) {
      return readers.get(table).getPrimaryKeyIndex();
    } else {
      return writers.get(table).getPrimaryKeyIndex();
    }
  }

  public IndexReader getIndex(Ref foreignKey) {
    String table = foreignKey.getRefTable().tableName();
    if (readers.containsKey(table)) {
      return readers.get(table).getIndex(foreignKey.getRefColumn());
    } else {
      return writers.get(table).getIndex(foreignKey.getRefColumn().getName());
    }
  }
}
