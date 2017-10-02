package com.cosyan.db.transaction;

import java.io.IOException;

import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.io.TableWriter;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableMap;

public class Resources {

  private final ImmutableMap<String, ExposedTableReader> readers;
  private final ImmutableMap<String, TableWriter> writers;

  public Resources(
      ImmutableMap<String, ExposedTableReader> readers,
      ImmutableMap<String, TableWriter> writers) {
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

  public ExposedTableReader reader(Ident table) throws IOException {
    if (readers.containsKey(table.getString())) {
      return readers.get(table.getString());
    } else {
      return writers.get(table.getString()).reader();
    }
  }
}
