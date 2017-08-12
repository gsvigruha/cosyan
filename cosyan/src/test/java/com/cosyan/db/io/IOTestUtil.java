package com.cosyan.db.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

public class IOTestUtil {

  public static class DummyTableReader extends ExposedTableReader {

    private final Iterator<Object[]> iterator;

    public DummyTableReader(
        ImmutableMap<String, ? extends ColumnMeta> columns,
        Object[][] data) {
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
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static class DummyMaterializedTableMeta extends ExposedTableMeta {

    private final ImmutableMap<String, BasicColumn> columns;
    private final Object[][] data;

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> columns() {
      return columns;
    }

    @Override
    public ExposedTableReader reader() throws ModelException {
      return new DummyTableReader(columns, data);
    }

    @Override
    public ImmutableMap<String, ? extends ColumnMeta> keyColumns() {
      return TableMeta.wholeTableKeys;
    }

    @Override
    public int indexOf(Ident ident) {
      return columns().keySet().asList().indexOf(ident.getString());
    }
  }
}
