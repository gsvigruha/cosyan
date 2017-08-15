package com.cosyan.db.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.cosyan.db.model.DataTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public abstract class TableReader {

  protected boolean cancelled = false;

  public abstract Object[] read() throws IOException;

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

    public MaterializedTableReader(
        DataInputStream inputStream, ImmutableMap<String, ? extends ColumnMeta> columns) {
      super(columns);
      this.inputStream = inputStream;
    }

    @Override
    public Object[] read() throws IOException {
      Object[] values = new Object[columns.size()];
      int i = 0; // ImmutableMap.entrySet() keeps iteration order.
      for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
        final Object value;
        if (entry.getValue().getType() == DataTypes.BoolType) {
          value = inputStream.readBoolean();
        } else if (entry.getValue().getType() == DataTypes.LongType) {
          value = inputStream.readLong();
        } else if (entry.getValue().getType() == DataTypes.DoubleType) {
          value = inputStream.readDouble();
        } else if (entry.getValue().getType() == DataTypes.StringType) {
          value = inputStream.readUTF();
        } else if (entry.getValue().getType() == DataTypes.DateType) {
          value = new Date(inputStream.readLong());
        } else {
          throw new UnsupportedOperationException();
        }
        values[i++] = value;
      }
      return values;
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

  public static class AggrTableReader extends TableReader {

    private final TableReader sourceReader;
    private final ColumnMeta havingColumn;
    private final ImmutableMap<String, ? extends ColumnMeta> keyColumns;
    private final ImmutableList<AggrColumn> aggrColumns;
    private final int size;

    private Iterator<Object[]> iterator;
    private boolean aggregated;

    public AggrTableReader(
        TableReader sourceReader,
        ImmutableMap<String, ? extends ColumnMeta> keyColumns,
        ImmutableList<AggrColumn> aggrColumns,
        ColumnMeta havingColumn) {
      this.sourceReader = sourceReader;
      this.keyColumns = keyColumns;
      this.aggrColumns = aggrColumns;
      this.havingColumn = havingColumn;
      this.aggregated = false;
      this.size = keyColumns.size() + aggrColumns.size();
    }

    @Override
    public Object[] read() throws IOException {
      if (!aggregated) {
        aggregate();
      }
      Object[] values = null;
      do {
        if (!iterator.hasNext()) {
          return null;
        }
        Object[] sourceValues = iterator.next();
        if (!(boolean) havingColumn.getValue(sourceValues)) {
          values = null;
        } else {
          values = sourceValues;
        }
      } while (values == null && !cancelled);
      return values;
    }

    private void aggregate() throws IOException {
      HashMap<ImmutableList<Object>, Object[]> aggregatedValues = new HashMap<>();
      while (!cancelled) {
        Object[] sourceValues = sourceReader.read();
        if (sourceValues == null) {
          break;
        }
        ImmutableList<Object> keyValues;
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        for (Map.Entry<String, ? extends ColumnMeta> entry : keyColumns.entrySet()) {
          builder.add(entry.getValue().getValue(sourceValues));
        }
        keyValues = builder.build();
        if (!aggregatedValues.containsKey(keyValues)) {
          Object[] aggrValues = new Object[aggrColumns.size()];
          int i = 0;
          for (AggrColumn column : aggrColumns) {
            aggrValues[i++] = column.getFunction().init();
          }
          aggregatedValues.put(keyValues, aggrValues);
        }
        Object[] aggrValues = aggregatedValues.get(keyValues);
        int i = 0;
        for (AggrColumn column : aggrColumns) {
          aggrValues[i] = column.getFunction().aggregate(aggrValues[i++], column.getInnerValue(sourceValues));
        }
      }
      for (Object[] values : aggregatedValues.values()) {
        int i = 0;
        for (AggrColumn column : aggrColumns) {
          values[i] = column.getFunction().finish(values[i++]);
        }
      }
      final Iterator<Entry<ImmutableList<Object>, Object[]>> innerIterator = aggregatedValues.entrySet().iterator();
      iterator = new Iterator<Object[]>() {

        @Override
        public boolean hasNext() {
          return innerIterator.hasNext();
        }

        @Override
        public Object[] next() {
          Object[] result = new Object[size];
          Entry<ImmutableList<Object>, Object[]> item = innerIterator.next();
          Object[] keys = item.getKey().toArray();
          Object[] values = item.getValue();
          System.arraycopy(keys, 0, result, 0, keys.length);
          System.arraycopy(values, 0, result, keys.length, values.length);
          return result;
        }
      };
      aggregated = true;
    }
  }
}
