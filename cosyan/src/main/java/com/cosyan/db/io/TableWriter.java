package com.cosyan.db.io;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class TableWriter {

  @Data
  public static class TableAppender {

    private final FileOutputStream fos;
    private final ImmutableList<BasicColumn> columns;

    public void write(Object[] values) throws IOException, ModelException {
      for (int i = 0; i < values.length; i++) {
        BasicColumn column = columns.get(i);
        if (!column.isNullable() && values[i] == DataTypes.NULL) {
          throw new ModelException("Column is not nullable.");
        }
      }
      fos.write(Serializer.write(values, columns));
    }

    public void close() throws IOException {
      fos.close();
    }
  }

  @Data
  public static class TableDeleter {

    private MappedByteBuffer buffer;
    private final ImmutableList<BasicColumn> columns;
    private final DerivedColumn whereColumn;

    public TableDeleter(MappedByteBuffer buffer, ImmutableList<BasicColumn> columns, DerivedColumn whereColumn) {
      this.buffer = buffer;
      this.columns = columns;
      this.whereColumn = whereColumn;
    }

    public void delete() throws IOException {
      do {
        buffer.mark();
        Object[] values = Serializer.read(columns, buffer);
        if (values == null) {
          return;
        }
        if ((boolean) whereColumn.getValue(values)) {
          int p = buffer.position();
          buffer.reset();
          buffer.put((byte) 0);
          buffer.position(p);
        }
      } while (buffer.hasRemaining());
    }

    public void close() throws IOException {
      buffer.force();
    }
  }
}
