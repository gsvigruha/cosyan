package com.cosyan.db.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.cosyan.db.conf.Config;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.RecordProvider.Record;
import com.cosyan.db.io.TableReader.IterableTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class IOTestUtil {

  public static class DummyTableReader extends SeekableTableReader {

    private final List<Object[]> data;

    public DummyTableReader(
        ImmutableMap<String, BasicColumn> columns,
        Object[][] data) throws IOException {
      super(null);
      this.data = Arrays.asList(data);
    }

    @Override
    public void close() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Record get(long position) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public IterableTableReader iterableReader(Resources resources) throws IOException {
      Iterator<Object[]> iterator = data.iterator();
      return new IterableTableReader() {

        @Override
        public Object[] next() throws IOException {
          if (iterator.hasNext()) {
            return iterator.next();
          } else {
            return null;
          }
        }

        @Override
        public void close() throws IOException {

        }
      };
    }

    @Override
    public IndexReader getIndex(String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Record get(Object key, Resources resources) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DummyMaterializedTableMeta extends MaterializedTableMeta {

    private final Object[][] data;

    public DummyMaterializedTableMeta(Config config, String name, ImmutableMap<String, BasicColumn> columns, Object[][] data) throws IOException, ConfigException {
      super(config, name, "admin", columns.values(), Optional.empty(), MaterializedTableMeta.Type.LOG);
      this.data = data;
    }

    public int indexOf(Ident ident) {
      return columns().keySet().asList().indexOf(ident.getString());
    }
  }
}
