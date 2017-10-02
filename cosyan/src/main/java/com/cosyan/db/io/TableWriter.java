package com.cosyan.db.io;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.SequenceInputStream;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.io.RecordReader.Record;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableMultiIndex;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

public class TableWriter implements TableIO {

  private final RandomAccessFile file;
  private final ImmutableMap<String, BasicColumn> columns;
  private final ImmutableMap<String, TableIndex> indexes;
  private final ImmutableMap<String, TableMultiIndex> multiIndexes;
  private final ImmutableMultimap<String, TableIndex> foreignIndexes;
  private final ImmutableMultimap<String, TableMultiIndex> reversedForeignIndexes;
  private final ImmutableMap<String, DerivedColumn> simpleChecks;

  private final long fileIndex0;
  private final Set<Long> recordsToDelete = new LinkedHashSet<>();
  private final ByteArrayOutputStream bos = new ByteArrayOutputStream();

  public TableWriter(
      RandomAccessFile file,
      ImmutableMap<String, BasicColumn> columns,
      ImmutableMap<String, TableIndex> indexes,
      ImmutableMap<String, TableMultiIndex> multiIndexes,
      ImmutableMultimap<String, TableIndex> foreignIndexes,
      ImmutableMultimap<String, TableMultiIndex> reversedForeignIndexes,
      ImmutableMap<String, DerivedColumn> simpleChecks) throws IOException {
    this.file = file;
    this.columns = columns;
    this.indexes = indexes;
    this.multiIndexes = multiIndexes;
    this.foreignIndexes = foreignIndexes;
    this.reversedForeignIndexes = reversedForeignIndexes;
    this.simpleChecks = simpleChecks;
    this.fileIndex0 = file.length();
  }

  public void insert(Object[] values) throws IOException, ModelException, IndexException {
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      long fileIndex = fileIndex0 + bos.size();
      BasicColumn column = columns.values().asList().get(i);
      if (!column.isNullable() && value == DataTypes.NULL) {
        throw new ModelException("Column is not nullable (mandatory).");
      }
      if (column.isUnique() && value != DataTypes.NULL) {
        indexes.get(column.getName()).put(value, fileIndex);
      }
      if (multiIndexes.containsKey(column.getName())) {
        multiIndexes.get(column.getName()).put(value, fileIndex);
      }
      if (foreignIndexes.containsKey(column.getName())) {
        for (TableIndex foreignIndex : foreignIndexes.get(column.getName())) {
          if (!foreignIndex.contains(value)) {
            throw new ModelException(String.format(
                "Foreign key violation, value '%s' not present.", value));
          }
        }
      }
    }
    for (Map.Entry<String, DerivedColumn> constraint : simpleChecks.entrySet()) {
      if (!(boolean) constraint.getValue().getValue(values)) {
        throw new ModelException("Constraint check " + constraint.getKey() + " failed.");
      }
    }
    Serializer.serialize(values, columns.values().asList(), bos);
  }

  public void commit() throws IOException {
    try {
      for (Long pos : recordsToDelete) {
        file.seek(pos);
        file.writeByte(0);
      }
      file.seek(fileIndex0);
      file.write(bos.toByteArray());
    } catch (IOException e) {
      rollback();
      file.getChannel().truncate(fileIndex0);
      throw e;
    }
    for (TableIndex index : indexes.values()) {
      try {
        index.commit();
      } catch (IOException e) {
        index.invalidate();
      }
    }
    for (TableMultiIndex index : multiIndexes.values()) {
      try {
        index.commit();
      } catch (IOException e) {
        index.invalidate();
      }
    }
  }

  public void rollback() {
    for (TableIndex index : indexes.values()) {
      index.rollback();
    }
    for (TableMultiIndex index : multiIndexes.values()) {
      index.rollback();
    }
  }

  public void close() throws IOException {
    bos.close();
    file.close();
  }

  private void delete(Record record) throws IOException, ModelException {
    recordsToDelete.add(record.getFilePointer());
    for (BasicColumn column : columns.values()) {
      Object value = record.getValues()[column.getIndex()];
      if (indexes.containsKey(column.getName())) {
        indexes.get(column.getName()).delete(value);
      }
      if (multiIndexes.containsKey(column.getName())) {
        multiIndexes.get(column.getName()).delete(value);
      }
      if (reversedForeignIndexes.containsKey(column.getName())) {
        for (TableMultiIndex reverseForeignIndex : reversedForeignIndexes.get(column.getName())) {
          if (reverseForeignIndex.contains(value)) {
            throw new ModelException(String.format(
                "Foreign key violation, key value '%s' has references.", value));
          }
        }
      }
    }
  }

  public long delete(DerivedColumn whereColumn) throws IOException, ModelException {
    long deletedLines = 0L;
    DataInputStream input = new DataInputStream(new SequenceInputStream(
        new PendingFile(file),
        new ByteArrayInputStream(bos.toByteArray())));
    RecordReader reader = new RecordReader(columns.values().asList(), input);
    do {
      Record record = reader.read();
      if (record == RecordReader.EMPTY) {
        return deletedLines;
      }
      if (!recordsToDelete.contains(record.getFilePointer()) && (boolean) whereColumn.getValue(record.getValues())) {
        delete(record);
        deletedLines++;
      }
    } while (true);
  }

  private ImmutableList<Object[]> deleteAndCollectUpdated(
      ImmutableMap<Integer, DerivedColumn> updateExprs,
      DerivedColumn whereColumn) throws IOException, ModelException {
    DataInput input = new DataInputStream(new SequenceInputStream(
        new PendingFile(file),
        new ByteArrayInputStream(bos.toByteArray())));
    RecordReader reader = new RecordReader(columns.values().asList(), input);
    ImmutableList.Builder<Object[]> updatedRecords = ImmutableList.builder();
    do {
      Record record = reader.read();
      if (record == RecordReader.EMPTY) {
        return updatedRecords.build();
      }
      if (!recordsToDelete.contains(record.getFilePointer()) && (boolean) whereColumn.getValue(record.getValues())) {
        delete(record);
        Object[] values = record.getValues();
        Object[] newValues = new Object[values.length];
        System.arraycopy(values, 0, newValues, 0, values.length);
        for (Map.Entry<Integer, DerivedColumn> updateExpr : updateExprs.entrySet()) {
          newValues[updateExpr.getKey()] = updateExpr.getValue().getValue(values);
        }
        updatedRecords.add(newValues);
      }
    } while (true);
  }

  public long update(ImmutableMap<Integer, DerivedColumn> columnExprs, DerivedColumn whereColumn)
      throws IOException, ModelException, IndexException {
    ImmutableList<Object[]> valuess = deleteAndCollectUpdated(columnExprs, whereColumn);
    for (Object[] values : valuess) {
      insert(values);
    }
    return valuess.size();
  }

  public ExposedTableReader reader() throws IOException {
    return new ExposedTableReader(columns) {

      private RecordReader reader = new RecordReader(
          columns.values().asList(),
          new DataInputStream(new SequenceInputStream(
              new PendingFile(file),
              new ByteArrayInputStream(bos.toByteArray()))));

      @Override
      public void close() throws IOException {

      }

      @Override
      public Object[] read() throws IOException {
        do {
          Record record = reader.read();
          if (record == RecordReader.EMPTY || !recordsToDelete.contains(record.getFilePointer())) {
            return record.getValues();
          }
        } while (true);
      }
    };
  }
}
