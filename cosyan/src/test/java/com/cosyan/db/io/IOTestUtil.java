package com.cosyan.db.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.SourceValues;
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
      super(ImmutableList.copyOf(columns.values()));
      this.iterator = Arrays.asList(data).iterator();
    }

    @Override
    public SourceValues read() throws IOException {
      if (!iterator.hasNext()) {
        return SourceValues.EMPTY;
      }
      return SourceValues.of(iterator.next());
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
      super("dummy", columns.values(), Optional.empty());
      this.data = data;
    }

    @Override
    public SeekableTableReader reader(Resources resources) throws IOException {
      return new DummyTableReader(columns(), data);
    }

    public int indexOf(Ident ident) {
      return columns().keySet().asList().indexOf(ident.getString());
    }

    @Override
    public MaterializedColumn getColumn(Ident ident) {
      BasicColumn column = columns().get(ident.getString());
      if (column == null) {
        return null;
      }
      return new MaterializedColumn(column, indexOf(ident), ImmutableList.of());
    }
  }
}
