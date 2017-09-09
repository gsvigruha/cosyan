package com.cosyan.db.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Optional;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.MaterializedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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

  public static void serialize(Object[] values, ImmutableList<? extends ColumnMeta> columns, OutputStream out)
      throws IOException {
    DataOutputStream stream = new DataOutputStream(out);
    stream.writeByte(1);
    for (int i = 0; i < columns.size(); i++) {
      Serializer.writeColumn(values[i], columns.get(i).getType(), stream);
    }
  }

  public static void writeTableMeta(
      MaterializedTableMeta tableMeta, OutputStream tableOut, OutputStream referencesOut) throws IOException {
    DataOutputStream tableStream = new DataOutputStream(tableOut);
    tableStream.writeInt(tableMeta.columns().size());
    for (BasicColumn column : tableMeta.columns().values()) {
      tableStream.writeUTF(column.getName());
      tableStream.writeUTF(column.getType().toString());
      tableStream.writeBoolean(column.isNullable());
      tableStream.writeBoolean(column.isUnique());
      tableStream.writeBoolean(column.isIndexed());
    }
    if (tableMeta.getPrimaryKey().isPresent()) {
      PrimaryKey primaryKey = tableMeta.getPrimaryKey().get();
      tableStream.writeByte(1);
      tableStream.writeUTF(primaryKey.getName());
      tableStream.writeUTF(primaryKey.getColumn().getName());
    } else {
      tableStream.writeByte(0);
    }
    DataOutputStream referencesStream = new DataOutputStream(referencesOut);
    referencesStream.writeInt(tableMeta.getForeignKeys().size());
    for (ForeignKey foreignKey : tableMeta.getForeignKeys().values()) {
      referencesStream.writeUTF(foreignKey.getName());
      referencesStream.writeUTF(foreignKey.getColumn().getName());
      referencesStream.writeUTF(foreignKey.getRefTable().getTableName());
      referencesStream.writeUTF(foreignKey.getRefColumn().getName());
    }
    referencesStream.writeInt(tableMeta.getReverseForeignKeys().size());
    for (ForeignKey reverseForeignKey : tableMeta.getReverseForeignKeys().values()) {
      referencesStream.writeUTF(reverseForeignKey.getName());
      referencesStream.writeUTF(reverseForeignKey.getColumn().getName());
      referencesStream.writeUTF(reverseForeignKey.getRefTable().getTableName());
      referencesStream.writeUTF(reverseForeignKey.getRefColumn().getName());
    }
  }

  public static MaterializedTableMeta readTableMeta(
      String tableName, InputStream tableIn, MetaRepo metaRepo) throws IOException {
    DataInputStream tableStream = new DataInputStream(tableIn);
    int numColumns = tableStream.readInt();
    ImmutableMap.Builder<String, BasicColumn> columnsBuilder = ImmutableMap.builder();
    for (int i = 0; i < numColumns; i++) {
      String columnName = tableStream.readUTF();
      columnsBuilder.put(columnName, new BasicColumn(
          i,
          columnName,
          DataTypes.fromString(tableStream.readUTF()),
          tableStream.readBoolean(),
          tableStream.readBoolean(),
          tableStream.readBoolean()));
    }
    ImmutableMap<String, BasicColumn> columns = columnsBuilder.build();
    byte hasPrimaryKey = tableStream.readByte();
    MaterializedTableMeta tableMeta = new MaterializedTableMeta(tableName, columns, metaRepo);
    if (hasPrimaryKey == 1) {
      tableMeta.setPrimaryKey(Optional.of(new PrimaryKey(
          tableStream.readUTF(),
          columns.get(tableStream.readUTF()))));
    }
    return tableMeta;
  }

  public static void readTableReferences(
      MaterializedTableMeta tableMeta, InputStream referencesIn, MetaRepo metaRepo) throws IOException, ModelException {
    DataInputStream referencesStream = new DataInputStream(referencesIn);
    tableMeta.setForeignKeys(readForeignKeys(tableMeta, referencesStream, metaRepo));
    tableMeta.setReverseForeignKeys(readForeignKeys(tableMeta, referencesStream, metaRepo));
  }

  private static ImmutableMap<String, ForeignKey> readForeignKeys(
      MaterializedTableMeta tableMeta, DataInputStream referencesStream, MetaRepo metaRepo)
      throws IOException, ModelException {
    int numForeignKeys = referencesStream.readInt();
    ImmutableMap.Builder<String, ForeignKey> foreignKeysBuilder = ImmutableMap.builder();
    for (int i = 0; i < numForeignKeys; i++) {
      String name = referencesStream.readUTF();
      String columnName = referencesStream.readUTF();
      MaterializedTableMeta refTable = metaRepo.table(new Ident(referencesStream.readUTF()));
      foreignKeysBuilder.put(name, new ForeignKey(
          name,
          tableMeta.columns().get(columnName),
          refTable,
          refTable.columns().get(referencesStream.readUTF())));
    }
    return foreignKeysBuilder.build();
  }
}
