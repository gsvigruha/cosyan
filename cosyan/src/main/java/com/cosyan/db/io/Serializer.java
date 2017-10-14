package com.cosyan.db.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Optional;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.DataTypes.DataType;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.sql.CreateStatement.SimpleCheckDefinition;
import com.cosyan.db.sql.Lexer;
import com.cosyan.db.sql.Parser;
import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

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
        throw new UnsupportedOperationException();
      }
    } else {
      throw new UnsupportedOperationException();
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
      MaterializedTableMeta tableMeta,
      OutputStream tableOut,
      OutputStream refOut) throws IOException {
    DataOutputStream tableStream = new DataOutputStream(tableOut);
    DataOutputStream refStream = new DataOutputStream(refOut);
    tableStream.writeInt(tableMeta.columns().size());
    for (BasicColumn column : tableMeta.columns().values()) {
      tableStream.writeUTF(column.getName());
      tableStream.writeUTF(column.getType().toString());
      tableStream.writeBoolean(column.isNullable());
      tableStream.writeBoolean(column.isUnique());
      tableStream.writeBoolean(column.isIndexed());
    }
    tableStream.writeInt(tableMeta.simpleChecks().size());
    for (SimpleCheckDefinition simpleCheck : tableMeta.simpleCheckDefinitions()) {
      tableStream.writeUTF(simpleCheck.getName());
      tableStream.writeUTF(simpleCheck.getExpr().print());
    }
    if (tableMeta.primaryKey().isPresent()) {
      PrimaryKey primaryKey = tableMeta.primaryKey().get();
      tableStream.writeByte(1);
      tableStream.writeUTF(primaryKey.getName());
      tableStream.writeUTF(primaryKey.getColumn().getName());
    } else {
      tableStream.writeByte(0);
    }

    refStream.writeInt(tableMeta.foreignKeys().size());
    for (ForeignKey foreignKey : tableMeta.foreignKeys().values()) {
      refStream.writeUTF(foreignKey.getName());
      refStream.writeUTF(foreignKey.getColumn().getName());
      refStream.writeUTF(foreignKey.getRefTable().tableName());
      refStream.writeUTF(foreignKey.getRefColumn().getName());
    }
  }

  public static MaterializedTableMeta readTableMeta(
      String tableName, InputStream tableIn, MetaRepo metaRepo) throws IOException, ParserException, ModelException {
    Parser parser = new Parser();
    Lexer lexer = new Lexer();
    DataInputStream tableStream = new DataInputStream(tableIn);
    int numColumns = tableStream.readInt();
    LinkedHashMap<String, BasicColumn> columns = Maps.newLinkedHashMap();
    for (int i = 0; i < numColumns; i++) {
      String columnName = tableStream.readUTF();
      columns.put(columnName, new BasicColumn(
          i,
          columnName,
          DataTypes.fromString(tableStream.readUTF()),
          tableStream.readBoolean(),
          tableStream.readBoolean(),
          tableStream.readBoolean()));
    }

    MaterializedTableMeta simpleTable = MaterializedTableMeta.simpleTable(tableName, columns);
    int numSimpleChecks = tableStream.readInt();
    ImmutableList.Builder<SimpleCheckDefinition> simpleCheckDefinitionsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<String, DerivedColumn> simpleChecksBuilder = ImmutableMap.builder();
    for (int i = 0; i < numSimpleChecks; i++) {
      SimpleCheckDefinition simpleCheckDefinition = new SimpleCheckDefinition(
          tableStream.readUTF(),
          parser.parseExpression(lexer.tokenize(tableStream.readUTF()), 0));
      simpleCheckDefinitionsBuilder.add(simpleCheckDefinition);
      simpleChecksBuilder.put(
          simpleCheckDefinition.getName(),
          simpleCheckDefinition.getExpr().compile(simpleTable));
    }
    byte hasPrimaryKey = tableStream.readByte();
    Optional<PrimaryKey> primaryKey;
    if (hasPrimaryKey == 1) {
      primaryKey = Optional.of(new PrimaryKey(
          tableStream.readUTF(),
          columns.get(tableStream.readUTF())));
    } else {
      primaryKey = Optional.empty();
    }
    return new MaterializedTableMeta(
        tableName,
        columns,
        simpleCheckDefinitionsBuilder.build(),
        simpleChecksBuilder.build(),
        primaryKey);
  }

  public static void readTableReferences(
      MaterializedTableMeta tableMeta, InputStream refIn, MetaRepo metaRepo) throws IOException, ModelException {
    DataInputStream refStream = new DataInputStream(refIn);
    int numForeignKeys = refStream.readInt();
    for (int i = 0; i < numForeignKeys; i++) {
      String name = refStream.readUTF();
      String columnName = refStream.readUTF();
      MaterializedTableMeta refTable = metaRepo.table(new Ident(refStream.readUTF()));
      ForeignKey foreignKey = new ForeignKey(
          name,
          tableMeta.columns().get(columnName),
          refTable,
          refTable.columns().get(refStream.readUTF()));
      tableMeta.addForeignKey(foreignKey);
      refTable.addReverseForeignKey(new ForeignKey(
              foreignKey.getName(),
              foreignKey.getRefColumn(),
              tableMeta,
              foreignKey.getColumn()));
    }
  }
}
