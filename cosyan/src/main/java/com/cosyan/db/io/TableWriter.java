package com.cosyan.db.io;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

import lombok.Data;

public class TableWriter {

  public static void writeColumn(
      Object value, DataType<?> dataType, DataOutputStream stream) throws IOException {
    if (value == DataTypes.NULL) {
      stream.writeByte(0);
      return;
    } else {
      stream.writeByte(1);
    }
    if (dataType == DataTypes.BoolType) {
      stream.writeBoolean((boolean) value);
    } else if (dataType == DataTypes.LongType) {
      stream.writeLong((long) value);
    } else if (dataType == DataTypes.DoubleType) {
      stream.writeDouble((double) value);
    } else if (dataType == DataTypes.StringType) {
      stream.writeUTF((String) value);
    } else if (dataType == DataTypes.DateType) {
      stream.writeLong(((Date) value).getTime());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Data
  public static class TableAppender {

    private final FileOutputStream fos;
    private final ImmutableList<BasicColumn> columns;

    public void write(Object[] values) throws IOException {
      ByteArrayOutputStream b = new ByteArrayOutputStream();
      DataOutputStream stream = new DataOutputStream(b);
      for (int i = 0; i < columns.size(); i++) {
        writeColumn(values[i], columns.get(i).getType(), stream);
      }
      fos.write(b.toByteArray());
    }
    
    public void close() throws IOException {
      fos.close();
    }
  }
}
