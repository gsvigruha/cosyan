package com.cosyan.db.io;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.ColumnMeta.DerivedColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableMultiIndex;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import lombok.Data;

public class TableWriter {

  @Data
  public static class TableAppender {

    private final FileOutputStream fos;
    private final ImmutableList<BasicColumn> columns;
    private final ImmutableMap<String, TableIndex> indexes;
    private final ImmutableMap<String, TableMultiIndex> multiIndexes;
    private final ImmutableMultimap<String, TableIndex> foreignIndexes;
    private final ImmutableMap<String, DerivedColumn> simpleChecks;
    private ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private long fileIndex0;

    public void init() throws IOException {
      fileIndex0 = fos.getChannel().position();
    }

    public void write(Object[] values) throws IOException, ModelException, IndexException {
      for (int i = 0; i < values.length; i++) {
        Object value = values[i];
        long fileIndex = fileIndex0 + bos.size();
        BasicColumn column = columns.get(i);
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
              throw new ModelException("Foreign key violation.");
            }
          }
        }
      }
      for (Map.Entry<String, DerivedColumn> constraint : simpleChecks.entrySet()) {
        if (!(boolean) constraint.getValue().getValue(values)) {
          throw new ModelException("Constraint check " + constraint.getKey() + " failed.");
        }
      }
      bos.write(Serializer.serialize(values, columns));
    }

    public void commit() throws IOException {
      for (TableIndex index : indexes.values()) {
        index.commit();
      }
      for (TableMultiIndex index : multiIndexes.values()) {
        index.commit();
      }
      for (TableIndex index : foreignIndexes.values()) {
        index.commit();
      }
      fos.write(bos.toByteArray());
      close();
    }

    public void rollback() throws IOException {
      for (TableIndex index : indexes.values()) {
        index.rollback();
      }
      for (TableMultiIndex index : multiIndexes.values()) {
        index.rollback();
      }
      for (TableIndex index : foreignIndexes.values()) {
        index.rollback();
      }
      close();
    }

    public void close() throws IOException {
      bos.close();
      fos.close();
    }
  }

  @Data
  public static class TableDeleter {

    private RandomAccessFile file;
    private final ImmutableList<BasicColumn> columns;
    private final DerivedColumn whereColumn;
    private final ImmutableMap<String, TableIndex> indexes;
    private final List<Long> recordsToDelete;

    public TableDeleter(
        RandomAccessFile file,
        ImmutableList<BasicColumn> columns,
        DerivedColumn whereColumn,
        ImmutableMap<String, TableIndex> indexes) {
      this.file = file;
      this.columns = columns;
      this.whereColumn = whereColumn;
      this.indexes = indexes;
      this.recordsToDelete = new LinkedList<>();
    }

    public void delete() throws IOException {
      do {
        long pos = file.getFilePointer();
        Object[] values = Serializer.read(columns, file);
        if (values == null) {
          return;
        }
        if ((boolean) whereColumn.getValue(values)) {
          recordsToDelete.add(pos);
          for (BasicColumn column : columns) {
            if (indexes.containsKey(column.getName())) {
              indexes.get(column.getName()).delete(values[column.getIndex()]);
            }
          }
        }
      } while (true);
    }

    public void commit() throws IOException {
      for (TableIndex index : indexes.values()) {
        index.commit();
      }
      for (Long pos : recordsToDelete) {
        file.seek(pos);
        file.writeByte(0);
      }
      close();
    }

    public void rollback() throws IOException {
      for (TableIndex index : indexes.values()) {
        index.rollback();
      }
      close();
    }

    public void close() throws IOException {
      file.close();
    }
  }

  @Data
  public static class TableDeleteAndCollector {

    private RandomAccessFile file;
    private final ImmutableList<BasicColumn> columns;
    private final ImmutableMap<Integer, DerivedColumn> updateExprs;
    private final DerivedColumn whereColumn;
    private final ImmutableMap<String, TableIndex> indexes;
    private final List<Long> recordsToDelete;

    public TableDeleteAndCollector(
        RandomAccessFile file,
        ImmutableList<BasicColumn> columns,
        ImmutableMap<Integer, DerivedColumn> updateExprs,
        DerivedColumn whereColumn,
        ImmutableMap<String, TableIndex> indexes) {
      this.file = file;
      this.columns = columns;
      this.updateExprs = updateExprs;
      this.whereColumn = whereColumn;
      this.indexes = indexes;
      this.recordsToDelete = new LinkedList<>();
    }

    public ImmutableList<Object[]> deleteAndCollect() throws IOException {
      ImmutableList.Builder<Object[]> updatedRecords = ImmutableList.builder();
      do {
        long pos = file.getFilePointer();
        Object[] values = Serializer.read(columns, file);
        if (values == null) {
          return updatedRecords.build();
        }
        if ((boolean) whereColumn.getValue(values)) {
          recordsToDelete.add(pos);
          for (BasicColumn column : columns) {
            if (indexes.containsKey(column.getName())) {
              indexes.get(column.getName()).delete(values[column.getIndex()]);
            }
          }
          Object[] newValues = new Object[values.length];
          System.arraycopy(values, 0, newValues, 0, values.length);
          for (Map.Entry<Integer, DerivedColumn> updateExpr : updateExprs.entrySet()) {
            newValues[updateExpr.getKey()] = updateExpr.getValue().getValue(values);
          }
          updatedRecords.add(newValues);
        }
      } while (true);
    }

    public void commit() throws IOException {
      for (TableIndex index : indexes.values()) {
        index.commit();
      }
      for (Long pos : recordsToDelete) {
        file.seek(pos);
        file.writeByte(0);
      }
      close();
    }

    public void rollback() throws IOException {
      for (TableIndex index : indexes.values()) {
        index.rollback();
      }
      close();
    }

    public void close() throws IOException {
      file.close();
    }
  }

  @Data
  public static class TableUpdater {
    private final TableDeleteAndCollector deleter;
    private final TableAppender appender;

    public boolean update() throws IOException, ModelException, IndexException {
      ImmutableList<Object[]> valuess = deleter.deleteAndCollect();
      for (Object[] values : valuess) {
        appender.write(values);
      }
      return true;
    }

    public void commit() throws IOException {
      deleter.commit();
      appender.commit();
    }

    public void rollback() throws IOException {
      deleter.rollback();
      appender.rollback();
      close();
    }

    public void close() throws IOException {
      deleter.close();
      appender.close();
    }
  }
}
