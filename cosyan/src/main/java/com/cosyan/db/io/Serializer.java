package com.cosyan.db.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

public class Serializer {

  public static Object[] read(ImmutableList<? extends ColumnMeta> columns, DataInput inputStream)
      throws IOException {
    do {
      final byte desc;
      try {
        desc = inputStream.readByte();
      } catch (EOFException e) {
        return null;
      }
      Object[] values = new Object[columns.size()];
      int i = 0; // ImmutableMap.entrySet() keeps iteration order.
      for (ColumnMeta column : columns) {
        values[i++] = readColumn(column.getType(), inputStream);
      }
      if (desc == 1) {
        return values;
      }
    } while (true);
  }

  public static Object readColumn(DataType<?> type, DataInput inputStream) throws IOException {
    final Object value;
    byte fieldDesc = inputStream.readByte();
    if (fieldDesc == 0) {
      value = DataTypes.NULL;
    } else if (fieldDesc == 1) {
      if (type == DataTypes.BoolType) {
        value = inputStream.readBoolean();
      } else if (type == DataTypes.LongType) {
        value = inputStream.readLong();
      } else if (type == DataTypes.DoubleType) {
        value = inputStream.readDouble();
      } else if (type == DataTypes.StringType) {
        int length = inputStream.readInt();
        char[] chars = new char[length];
        for (int c = 0; c < chars.length; c++) {
          chars[c] = inputStream.readChar();
        }
        value = new String(chars);
      } else if (type == DataTypes.DateType) {
        value = new Date(inputStream.readLong());
      } else {
        throw new UnsupportedOperationException();
      }
    } else {
      throw new UnsupportedOperationException();
    }
    return value;
  }

  public static void writeColumn(Object value, DataType<?> dataType, DataOutput stream) throws IOException {
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
      String str = (String) value;
      stream.writeInt(str.length());
      stream.writeChars(str);
    } else if (dataType == DataTypes.DateType) {
      stream.writeLong(((Date) value).getTime());
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public static byte[] serialize(Object[] values, ImmutableList<? extends ColumnMeta> columns) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    DataOutputStream stream = new DataOutputStream(b);
    stream.writeByte(1);
    for (int i = 0; i < columns.size(); i++) {
      Serializer.writeColumn(values[i], columns.get(i).getType(), stream);
    }
    return b.toByteArray();
  }

  public static void serialize(Object[] values, ImmutableList<? extends ColumnMeta> columns, OutputStream out) throws IOException {
    DataOutputStream stream = new DataOutputStream(out);
    stream.writeByte(1);
    for (int i = 0; i < columns.size(); i++) {
      Serializer.writeColumn(values[i], columns.get(i).getType(), stream);
    }
  }
}
