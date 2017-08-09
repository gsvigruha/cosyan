package com.cosyan.db.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.google.common.collect.ImmutableMap;

import lombok.Data;

@Data
public abstract class TableReader {

  protected final ImmutableMap<String, ? extends ColumnMeta> columns;

  public abstract ImmutableMap<String, Object> read() throws IOException;

  public static class MaterializedTableReader extends TableReader {

    private final DataInputStream inputStream;

    public MaterializedTableReader(
        DataInputStream inputStream, ImmutableMap<String, ? extends ColumnMeta> columns) {
      super(columns);
      this.inputStream = inputStream;
    }

    @Override
    public ImmutableMap<String, Object> read() throws IOException {
      ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
      for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
        final Object value;
        if (entry.getValue().getType() == DataTypes.BoolType) {
          value = inputStream.readBoolean();
        } else if (entry.getValue().getType() == DataTypes.LongType) {
          value = inputStream.readLong();
        } else if (entry.getValue().getType() == DataTypes.DoubleType) {
          value = inputStream.readDouble();
        } else if (entry.getValue().getType() == DataTypes.StringType) {
          value = inputStream.readUTF();
        } else if (entry.getValue().getType() == DataTypes.DateType) {
          value = new Date(inputStream.readLong());
        } else {
          throw new UnsupportedOperationException();
        }
        values.put(entry.getKey(), value);
      }
      return values.build();
    }
  }

  public static class DerivedTableReader extends TableReader {

    private final TableReader sourceReader;

    public DerivedTableReader(
        TableReader sourceReader, ImmutableMap<String, ?extends ColumnMeta> columns) {
      super(columns);
      this.sourceReader = sourceReader;
    }

    @Override
    public ImmutableMap<String, Object> read() throws IOException {
      ImmutableMap<String, Object> sourceValues = sourceReader.read();
      ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
      for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
        values.put(entry.getKey(), entry.getValue().getValue(sourceValues));
      }
      return values.build();
    }
  }
}
