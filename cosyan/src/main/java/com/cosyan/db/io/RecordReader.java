package com.cosyan.db.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.SourceValues;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class RecordReader {

  @Data
  public static class Record {
    private final long filePointer;
    private final Object[] values;

    public SourceValues sourceValues() {
      return SourceValues.of(values);
    }
  }

  private static class EmptyRecord extends Record {

    public EmptyRecord() {
      super(-1, null);
    }

    @Override
    public SourceValues sourceValues() {
      return SourceValues.EMPTY;
    }
  }

  public static final Record EMPTY = new EmptyRecord();

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
        if (pointer - recordPointer == recordSize + 5) {
          // Pointer is at the end of the supposed length of the record.
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
