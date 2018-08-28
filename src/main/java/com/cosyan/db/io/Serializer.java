/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;

import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes.DataType;
import com.google.common.collect.ImmutableList;

public class Serializer {

  public static Object readColumn(DataType<?> type, DataInput inputStream) throws IOException {
    final Object value;
    byte fieldDesc = inputStream.readByte();
    if (fieldDesc == 0) {
      value = null;
    } else if (fieldDesc == 1) {
      return type.read(inputStream);
    } else {
      throw new IOException(String.format("Invalid record header %s.", fieldDesc));
    }
    return value;
  }

  public static int size(DataType<?> type, Object value) {
    if (value == null) {
      return 1;
    }
    return type.size(value) + 1;
  }

  public static void writeColumn(Object value, DataType<?> dataType, DataOutput stream) throws IOException {
    if (value == null) {
      stream.writeByte(0);
      return;
    } else {
      stream.writeByte(1);
    }
    dataType.write(value, stream);
  }

  public static byte[] serialize(Object[] values, ImmutableList<BasicColumn> columns)
      throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
    serialize(values, columns, bos);
    return bos.toByteArray();
  }

  private static void serialize(Object[] values, ImmutableList<BasicColumn> columns, ByteArrayOutputStream out)
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
    out.write(record);
    CRC32 checksum = new CRC32();
    checksum.update(record);
    stream.writeInt((int) checksum.getValue());
  }
}
