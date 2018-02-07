package com.cosyan.db.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Set;

import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import lombok.Data;

public interface RecordProvider {

  public void close() throws IOException;

  public Record read() throws IOException;

  @Data
  public static class Record {
    private final long filePointer;
    private final Object[] values;
  }

  public static class EmptyRecord extends Record {

    public EmptyRecord() {
      super(-1, null);
    }
  }

  public static final Record EMPTY = new EmptyRecord();

  public class RecordReader implements RecordProvider {

    private final ImmutableList<BasicColumn> columns;
    private final Set<Long> recordsToDelete;
    private final int numColumns;
    private final SeekableInputStream inputStream;
    private final DataInput dataInput;
    private long pointer;

    public RecordReader(
        ImmutableList<BasicColumn> columns,
        SeekableInputStream inputStream,
        Set<Long> recordsToDelete) {
      this.columns = columns;
      this.recordsToDelete = recordsToDelete;
      this.numColumns = (int) columns.stream().filter(column -> !column.isDeleted()).count();
      this.inputStream = inputStream;
      this.dataInput = new DataInputStream(inputStream);
      this.pointer = 0L;
    }

    public RecordReader(
        ImmutableList<BasicColumn> columns,
        SeekableInputStream inputStream) {
      this(columns, inputStream, ImmutableSet.of());
    }

    @Override
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
        if (desc == 1 && !recordsToDelete.contains(recordPointer)) {
          return new Record(recordPointer, values);
        }
      } while (true);
    }

    public void seek(long position) throws IOException {
      if (recordsToDelete.contains(position)) {
        throw new IOException("Record " + position + " is deleted.");
      }
      inputStream.seek(position);
      pointer = position;
    }

    public Object position() {
      return pointer;
    }

    @Override
    public void close() throws IOException {
      inputStream.close();
    }
  }
}
