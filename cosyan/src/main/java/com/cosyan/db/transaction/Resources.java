package com.cosyan.db.transaction;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Optional;

import com.cosyan.db.io.DependencyReader;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.MaterializedTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.PrimaryKey;
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

    public SeekableTableReader create(DependencyReader dependencyReader) throws IOException {
      return new MaterializedTableReader(
          new RandomAccessFile(fileName, "rw"),
          columns,
          indexReaders,
          dependencyReader);
    }

    public TableIndex getPrimaryKeyIndex() {
      return (TableIndex) indexReaders.get(primaryKey.get().getColumn().getName());
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

  public TableWriter writer(Ident table, DependencyReader dependencyReader) {
    TableWriter writer = writers.get(table.getString());
    writer.setDependencyReader(dependencyReader);
    return writer;
  }

  public SeekableTableReader createReader(Ident table) throws IOException {
    if (readers.containsKey(table.getString())) {
      return readers.get(table.getString()).create(DependencyReader.NO_DEPS);
    } else {
      return writers.get(table.getString()).createReader(DependencyReader.NO_DEPS);
    }
  }

  public SeekableTableReader createReader(Ident table, DependencyReader dependencyReader) throws IOException {
    if (readers.containsKey(table.getString())) {
      return readers.get(table.getString()).create(dependencyReader);
    } else {
      return writers.get(table.getString()).createReader(dependencyReader);
    }
  }

  public TableIndex getPrimaryKeyIndex(Ident table) {
    if (readers.containsKey(table.getString())) {
      return readers.get(table.getString()).getPrimaryKeyIndex();
    } else {
      return writers.get(table.getString()).getPrimaryKeyIndex();
    }
  }
}
