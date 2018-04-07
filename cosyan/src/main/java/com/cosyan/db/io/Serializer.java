package com.cosyan.db.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.zip.CRC32;

import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

public class Serializer {

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
        throw new RuntimeException(String.format("Unknown data type %s.", type));
      }
    } else {
      throw new IOException(String.format("Invalid record header %s.", fieldDesc));
    }
    return value;
  }

  public static int size(DataType<?> type, Object value) {
    if (value == DataTypes.NULL) {
      return 1;
    }
    if (type == DataTypes.BoolType) {
      return 2;
    } else if (type == DataTypes.LongType) {
      return 9;
    } else if (type == DataTypes.DoubleType) {
      return 9;
    } else if (type == DataTypes.StringType) {
      return 5 + ((String) value).length() * 2;
    } else if (type == DataTypes.DateType) {
      return 9;
    } else {
      throw new UnsupportedOperationException();
    }
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

  public static byte[] serialize(Object[] values, ImmutableList<BasicColumn> columns)
      throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    serialize(values, columns, bos);
    return bos.toByteArray();
  }

  public static void serialize(Object[] values, ImmutableList<BasicColumn> columns, OutputStream out)
      throws IOException {
    DataOutputStream stream = new DataOutputStream(out);
    stream.writeByte(1);
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    DataOutputStream recordStream = new DataOutputStream(bos);
    int i = 0;
    for (BasicColumn column : columns) {
      if (!column.isDeleted()) {
        Serializer.writeColumn(values[i++], column.getType(), recordStream);
      } else {
        recordStream.writeByte(0);
      }
    }
    stream.writeInt(bos.size());
    byte[] record = bos.toByteArray();
    stream.write(record);
    CRC32 checksum = new CRC32();
    checksum.update(record);
    stream.writeInt((int) checksum.getValue());
  }
}
