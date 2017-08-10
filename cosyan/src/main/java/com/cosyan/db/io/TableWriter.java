package com.cosyan.db.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;

public class TableWriter {

  public static void write(
      Object value, DataType<?> dataType, DataOutputStream stream) throws IOException {
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
}
