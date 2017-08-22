package com.cosyan.db.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Date;

import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

public class Serializer {

  public static Object[] read(ImmutableList<? extends ColumnMeta> columns, DataInputStream inputStream)
      throws IOException {
    do {
      if (inputStream.available() == 0) {
        return null;
      }
      byte desc = inputStream.readByte();
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

  public static Object[] read(ImmutableList<BasicColumn> columns, MappedByteBuffer buffer) {
    do {
      if (!buffer.hasRemaining()) {
        return null;
      }
      byte desc = buffer.get();
      Object[] values = new Object[columns.size()];
      int i = 0; // ImmutableMap.entrySet() keeps iteration order.
      for (ColumnMeta column : columns) {
        final Object value;
        byte fieldDesc = buffer.get();
        if (fieldDesc == 0) {
          value = DataTypes.NULL;
        } else if (fieldDesc == 1) {
          if (column.getType() == DataTypes.BoolType) {
            value = buffer.get() == 1;
          } else if (column.getType() == DataTypes.LongType) {
            value = buffer.getLong();
          } else if (column.getType() == DataTypes.DoubleType) {
            value = buffer.getDouble();
          } else if (column.getType() == DataTypes.StringType) {
            int length = buffer.getInt();
            char[] chars = new char[length];
            for (int c = 0; c < chars.length; c++) {
              chars[c] = buffer.getChar();
            }
            value = new String(chars);
          } else if (column.getType() == DataTypes.DateType) {
            value = new Date(buffer.getLong());
          } else {
            throw new UnsupportedOperationException();
          }
        } else {
          throw new UnsupportedOperationException();
        }

        values[i++] = value;
      }
      if (desc == 1) {
        return values;
      }
    } while (true);
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

  public static byte[] write(Object[] values, ImmutableList<? extends ColumnMeta> columns) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    DataOutputStream stream = new DataOutputStream(b);
    stream.writeByte(1);
    for (int i = 0; i < columns.size(); i++) {
      Serializer.writeColumn(values[i], columns.get(i).getType(), stream);
    }
    return b.toByteArray();
  }
}
