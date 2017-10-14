package com.cosyan.db.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class IOTestUtil {

  public static class DummyTableReader extends SeekableTableReader {

    private final Iterator<Object[]> iterator;

    public DummyTableReader(
        ImmutableMap<String, BasicColumn> columns,
        Object[][] data) throws IOException {
      super(columns);
      this.iterator = Arrays.asList(data).iterator();
    }

    @Override
    public Object[] read() throws IOException {
      if (!iterator.hasNext()) {
        return null;
      }
      return iterator.next();
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void seek(long position) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexReader indexReader(Ident ident) {
      throw new UnsupportedOperationException();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DummyMaterializedTableMeta extends MaterializedTableMeta {

    private final Object[][] data;

    public DummyMaterializedTableMeta(ImmutableMap<String, BasicColumn> columns, Object[][] data) {
      super("dummy", columns.values(), ImmutableList.of(), ImmutableMap.of(), Optional.empty());
      this.data = data;
    }

    @Override
    public SeekableTableReader reader(Resources resources) throws IOException {
      return new DummyTableReader(columns(), data);
    }

    @Override
    public int indexOf(Ident ident) {
      return columns().keySet().asList().indexOf(ident.getString());
    }

    @Override
    public BasicColumn column(Ident ident) {
      return columns().get(ident.getString());
    }
  }
}
