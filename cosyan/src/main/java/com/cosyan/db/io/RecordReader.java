package com.cosyan.db.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class RecordReader {

  @Data
  public static class Record {
    private final long filePointer;
    private final Object[] values;
  }

  public static final Record EMPTY = new Record(-1, null);

  private final ImmutableList<BasicColumn> columns;
  private final int numColumns;
  private final SeekableInputStream inputStream;
  private final DataInput dataInput;
  private long pointer;

  public RecordReader(ImmutableList<BasicColumn> columns, SeekableInputStream inputStream) {
    this.columns = columns;
    this.numColumns = (int) columns.stream().filter(column -> !column.isDeleted()).count();
    this.inputStream = inputStream;
    this.dataInput = new DataInputStream(inputStream);
    this.pointer = 0L;
  }

  public Record read() throws IOException {
    do {
      long recordPointer = pointer;
      final byte desc;
      try {
        desc = dataInput.readByte();
        pointer++;
      } catch (EOFException e) {
        return EMPTY;
      }
      int recordSize = dataInput.readInt();
      pointer += 4;
      Object[] values = new Object[numColumns];
      int i = 0;
      for (BasicColumn column : columns) {
        Object value = Serializer.readColumn(column.getType(), dataInput);
        if (!column.isDeleted()) {
          values[i++] = value;
        }
        pointer += Serializer.size(column.getType(), value);
        if (pointer - recordPointer == recordSize) {
          break;
        }
      }
      for (int j = i; j < numColumns; j++) {
        values[j] = DataTypes.NULL;
      }
      dataInput.readInt(); // CRC;
      pointer += 4;
      if (desc == 1) {
        return new Record(recordPointer, values);
      }
    } while (true);
  }

  public void seek(long position) throws IOException {
    inputStream.seek(position);
    pointer = position;
  }
}
