package com.cosyan.db.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.cosyan.db.io.RecordReader.Record;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public abstract class TableReader {

  protected boolean cancelled = false;

  public abstract Object[] read() throws IOException;

  public abstract void close() throws IOException;

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static abstract class ExposedTableReader extends TableReader {

    protected final ImmutableMap<String, ? extends ColumnMeta> columns;

    public ImmutableMap<String, Object> readColumns() throws IOException {
      Object[] values = read();
      if (values == null) {
        return null;
      }
      ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
      int i = 0;
      for (String columnName : columns.keySet()) {
        builder.put(columnName, values[i++]);
      }
      return builder.build();
    }
  }

  public static class MaterializedTableReader extends ExposedTableReader {

    private final DataInputStream inputStream;
    private final RecordReader reader;

    public MaterializedTableReader(
        DataInputStream inputStream, ImmutableMap<String, ? extends ColumnMeta> columns) {
      super(columns);
      this.inputStream = inputStream;
      this.reader = new RecordReader(columns.values().asList(), inputStream);
    }

    @Override
    public void close() throws IOException {
      inputStream.close();
    }

    @Override
    public Object[] read() throws IOException {
      Record record = reader.read();
      if (record == RecordReader.EMPTY) {
        close();
      }
      return record.getValues();
    }
  }

  public static class DerivedTableReader extends ExposedTableReader {

    private final TableReader sourceReader;

    public DerivedTableReader(
        TableReader sourceReader,
        ImmutableMap<String, ? extends ColumnMeta> columns) {
      super(columns);
      this.sourceReader = sourceReader;
    }

    @Override
    public void close() throws IOException {
      sourceReader.close();
    }

    @Override
    public Object[] read() throws IOException {
      Object[] values = null;
      Object[] sourceValues = sourceReader.read();
      if (sourceValues == null) {
        return null;
      } else {
        values = new Object[columns.size()];
        int i = 0;
        for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
          values[i++] = entry.getValue().getValue(sourceValues);
        }
      }
      return values;
    }
  }

  public static class FilteredTableReader extends ExposedTableReader {

    private final ExposedTableReader sourceReader;
    private final ColumnMeta whereColumn;

    public FilteredTableReader(
        ExposedTableReader sourceReader,
        ColumnMeta whereColumn) {
      super(sourceReader.columns);
      this.sourceReader = sourceReader;
      this.whereColumn = whereColumn;
    }

    @Override
    public void close() throws IOException {
      sourceReader.close();
    }

    @Override
    public Object[] read() throws IOException {
      Object[] values = null;
      do {
        Object[] sourceValues = sourceReader.read();
        if (sourceValues == null) {
          return null;
        } else {
          if (!(boolean) whereColumn.getValue(sourceValues)) {
            values = null;
          } else {
            values = sourceValues;
          }
        }
      } while (values == null && !cancelled);
      return values;
    }
  }

  public static class SortedTableReader extends ExposedTableReader {

    private final ExposedTableReader sourceReader;
    private final ImmutableList<OrderColumn> orderColumns;
    private boolean sorted;
    private Iterator<Object[]> iterator;

    public SortedTableReader(
        ExposedTableReader sourceReader,
        ImmutableList<OrderColumn> orderColumns) {
      super(sourceReader.columns);
      this.sourceReader = sourceReader;
      this.orderColumns = orderColumns;
      this.sorted = false;
    }

    @Override
    public void close() throws IOException {
      sourceReader.close();
    }

    @Override
    public Object[] read() throws IOException {
      if (!sorted) {
        sort();
      }
      if (!iterator.hasNext()) {
        return null;
      }
      return iterator.next();
    }

    private void sort() throws IOException {
      TreeMap<ImmutableList<Object>, Object[]> values = new TreeMap<>(new Comparator<ImmutableList<Object>>() {
        @Override
        public int compare(ImmutableList<Object> x, ImmutableList<Object> y) {
          for (int i = 0; i < orderColumns.size(); i++) {
            int result = orderColumns.get(i).compare(x.get(i), y.get(i));
            if (result != 0) {
              return result;
            }
          }
          return 0;
        }
      });
      while (!cancelled) {
        Object[] sourceValues = sourceReader.read();
        if (sourceValues == null) {
          break;
        }
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        for (OrderColumn column : orderColumns) {
          Object key = column.getValue(sourceValues);
          builder.add(key);
        }
        values.put(builder.build(), sourceValues);
      }
      iterator = values.values().iterator();
      sorted = true;
    }
  }

  public static class HashJoinTableReader extends ExposedTableReader {

    private final ExposedTableReader mainTableReader;
    private final ExposedTableReader joinTableReader;
    private final ImmutableList<ColumnMeta> mainTableJoinColumns;
    private final ImmutableList<ColumnMeta> joinTableJoinColumns;
    private final boolean mainTableFirst;

    private boolean joined;
    private LinkedListMultimap<ImmutableList<Object>, Object[]> joinValues;
    private Iterator<Object[]> joinValuesForCurrentKey;
    private Object[] mainTableValues;

    public HashJoinTableReader(
        ExposedTableReader mainTableReader,
        ExposedTableReader joinTableReader,
        ImmutableList<ColumnMeta> mainTableJoinColumns,
        ImmutableList<ColumnMeta> joinTableJoinColumns,
        boolean mainTableFirst) {
      super(null); // TODO fix design here
      this.mainTableReader = mainTableReader;
      this.joinTableReader = joinTableReader;
      this.mainTableJoinColumns = mainTableJoinColumns;
      this.joinTableJoinColumns = joinTableJoinColumns;
      this.mainTableFirst = mainTableFirst;
      this.joined = false;
    }

    @Override
    public void close() throws IOException {
      mainTableReader.close();
      joinTableReader.close();
    }

    @Override
    public Object[] read() throws IOException {
      if (!joined) {
        join();
      }
      Object[] result = null;
      do {
        while (joinValuesForCurrentKey == null || !joinValuesForCurrentKey.hasNext()) {
          List<Object[]> values = null;
          mainTableValues = mainTableReader.read();
          if (mainTableValues == null) {
            return null;
          }
          ImmutableList.Builder<Object> builder = ImmutableList.builder();
          for (ColumnMeta column : mainTableJoinColumns) {
            Object key = column.getValue(mainTableValues);
            builder.add(key);
          }
          values = joinValues.get(builder.build());
          if (values != null) {
            joinValuesForCurrentKey = values.iterator();
          }
        }

        result = match(mainTableValues, joinValuesForCurrentKey.next());
      } while (result == null);
      return result;
    }

    private Object[] match(Object[] mainTableValues, Object[] joinTableValues) {
      Object[] result = new Object[mainTableReader.columns.size() + joinTableReader.columns.size()];
      if (joinTableValues == null) {
        return null;
      }
      if (mainTableFirst) {
        System.arraycopy(mainTableValues, 0, result, 0, mainTableValues.length);
        System.arraycopy(joinTableValues, 0, result, mainTableValues.length, joinTableValues.length);
      } else {
        System.arraycopy(joinTableValues, 0, result, 0, joinTableValues.length);
        System.arraycopy(mainTableValues, 0, result, joinTableValues.length, mainTableValues.length);
      }
      return result;
    }

    private void join() throws IOException {
      joinValues = LinkedListMultimap.create();
      while (!cancelled) {
        Object[] sourceValues = joinTableReader.read();
        if (sourceValues == null) {
          break;
        }
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        for (ColumnMeta column : joinTableJoinColumns) {
          Object key = column.getValue(sourceValues);
          builder.add(key);
        }
        joinValues.put(builder.build(), sourceValues);
      }
      joined = true;
    }
  }
}
