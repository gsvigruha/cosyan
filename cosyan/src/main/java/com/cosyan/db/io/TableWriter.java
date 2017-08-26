package com.cosyan.db.io;

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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;

import lombok.Data;

public class TableWriter {

  @Data
  public static class TableAppender {

    private final FileOutputStream fos;
    private final ImmutableList<BasicColumn> columns;
    private final ImmutableMap<String, TableIndex> indexes;
    private final ImmutableMap<String, DerivedColumn> constraints;
    private List<Object[]> valuess = new LinkedList<>();

    public void write(Object[] values) throws IOException, ModelException, IndexException {
      for (int i = 0; i < values.length; i++) {
        BasicColumn column = columns.get(i);
        if (!column.isNullable() && values[i] == DataTypes.NULL) {
          throw new ModelException("Column is not nullable (mandatory).");
        }
        if (column.isUnique() && values[i] != DataTypes.NULL) {
          indexes.get(column.getName()).put(values[i], fos.getChannel().position());
        }
      }
      for (Map.Entry<String, DerivedColumn> constraint : constraints.entrySet()) {
        if (!(boolean) constraint.getValue().getValue(values)) {
          throw new ModelException("Constraint check " + constraint.getKey() + " failed.");
        }
      }
      this.valuess.add(values);
    }

    public void commit() throws IOException {
      for (Object[] values : valuess) {
        fos.write(Serializer.write(values, columns));
      }
      close();
    }

    public void rollback() throws IOException {
      close();
    }

    public void close() throws IOException {
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
    private final ListMultimap<String, Object> indexesToDelete;

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
      this.indexesToDelete = LinkedListMultimap.create();
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
              indexesToDelete.put(column.getName(), values[column.getIndex()]);
            }
          }
        }
      } while (true);
    }

    public void commit() throws IOException {
      for (Long pos : recordsToDelete) {
        file.seek(pos);
        file.writeByte(0);
      }
      for (String indexName : indexesToDelete.keySet()) {
        TableIndex index = indexes.get(indexName);
        for (Object key : indexesToDelete.get(indexName)) {
          index.delete(key);
        }
      }
      close();
    }

    public void rollback() throws IOException {
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
    private final ListMultimap<String, Object> indexesToDelete;

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
      this.indexesToDelete = LinkedListMultimap.create();
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
              indexesToDelete.put(column.getName(), values[column.getIndex()]);
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
      for (Long pos : recordsToDelete) {
        file.seek(pos);
        file.writeByte(0);
      }
      for (String indexName : indexesToDelete.keySet()) {
        TableIndex index = indexes.get(indexName);
        for (Object key : indexesToDelete.get(indexName)) {
          index.delete(key);
        }
      }
      close();
    }

    public void rollback() throws IOException {
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
      close();
    }

    public void close() throws IOException {
      deleter.close();
      appender.close();
    }
  }
}
