package com.cosyan.db.io;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Map;

import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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

    private MappedDataFile file;
    private final ImmutableList<BasicColumn> columns;
    private final DerivedColumn whereColumn;

    public TableDeleter(MappedDataFile file, ImmutableList<BasicColumn> columns, DerivedColumn whereColumn) {
      this.file = file;
      this.columns = columns;
      this.whereColumn = whereColumn;
    }

    public void delete() throws IOException {
      MappedByteBuffer buffer = file.getBuffer();
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
      file.close();
    }
  }

  @Data
  public static class TableDeleteAndCollector {

    private MappedDataFile file;
    private final ImmutableList<BasicColumn> columns;
    private final ImmutableMap<Integer, DerivedColumn> updateExprs;
    private final DerivedColumn whereColumn;

    public TableDeleteAndCollector(
        MappedDataFile file,
        ImmutableList<BasicColumn> columns,
        ImmutableMap<Integer, DerivedColumn> updateExprs,
        DerivedColumn whereColumn) {
      this.file = file;
      this.columns = columns;
      this.updateExprs = updateExprs;
      this.whereColumn = whereColumn;
    }

    public ImmutableList<Object[]> deleteAndCollect() throws IOException {
      MappedByteBuffer buffer = file.getBuffer();
      ImmutableList.Builder<Object[]> updatedRecords = ImmutableList.builder();
      do {
        buffer.mark();
        Object[] values = Serializer.read(columns, buffer);
        if (values == null) {
          return updatedRecords.build();
        }
        if ((boolean) whereColumn.getValue(values)) {
          int p = buffer.position();
          buffer.reset();
          buffer.put((byte) 0);
          buffer.position(p);
          Object[] newValues = new Object[values.length];
          System.arraycopy(values, 0, newValues, 0, values.length);
          for (Map.Entry<Integer, DerivedColumn> updateExpr : updateExprs.entrySet()) {
            newValues[updateExpr.getKey()] = updateExpr.getValue().getValue(values);
          }
          updatedRecords.add(newValues);
        }
      } while (buffer.hasRemaining());
      return updatedRecords.build();
    }

    public void close() throws IOException {
      file.close();
    }
  }

  @Data
  public static class TableUpdater {
    private final TableDeleteAndCollector deleter;
    private final TableAppender appender;

    public boolean update() throws IOException, ModelException {
      ImmutableList<Object[]> valuess = deleter.deleteAndCollect();
      deleter.close();
      for (Object[] values : valuess) {
        appender.write(values);
      }
      return true;
    }

    public void close() throws IOException {
      deleter.close();
      appender.close();
    }
  }
}
