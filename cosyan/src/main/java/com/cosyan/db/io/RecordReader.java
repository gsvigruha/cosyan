package com.cosyan.db.io;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

import com.cosyan.db.model.ColumnMeta;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class RecordReader {

  @Data
  public static class Record {
    private final long filePointer;
    private final Object[] values;
  }

  public static final Record EMPTY = new Record(-1, null);

  private final ImmutableList<? extends ColumnMeta> columns;
  private final DataInput inputStream;
  private long pointer;

  public RecordReader(ImmutableList<? extends ColumnMeta> columns, DataInput inputStream) {
    this.columns = columns;
    this.inputStream = inputStream;
    this.pointer = 0L;
  }

  public Record read() throws IOException {
    do {
      long recordPointer = pointer;
      final byte desc;
      try {
        desc = inputStream.readByte();
        pointer++;
      } catch (EOFException e) {
        return EMPTY;
      }
      Object[] values = new Object[columns.size()];
      int i = 0; // ImmutableMap.entrySet() keeps iteration order.
      for (ColumnMeta column : columns) {
        Object value = Serializer.readColumn(column.getType(), inputStream); 
        values[i++] = value;
        pointer += Serializer.size(column.getType(), value);
      }
      if (desc == 1) {
        return new Record(recordPointer, values);
      }
    } while (true);
  }
}
