package com.cosyan.db.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public abstract class Aggregation extends TableReader {

  protected final TableReader sourceReader;
  protected final ColumnMeta havingColumn;
  protected final ImmutableList<AggrColumn> aggrColumns;
  protected final int size;

  protected Iterator<Object[]> iterator;
  protected boolean aggregated;

  protected Aggregation(
      TableReader sourceReader,
      ImmutableList<AggrColumn> aggrColumns,
      ColumnMeta havingColumn,
      int size) {
    this.sourceReader = sourceReader;
    this.aggrColumns = aggrColumns;
    this.havingColumn = havingColumn;
    this.aggregated = false;
    this.size = size;
  }

  @Override
  public void close() throws IOException {
    sourceReader.close();
  }

  protected abstract void aggregate() throws IOException;

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

  public static class KeyValueAggrTableReader extends Aggregation {

    private final ImmutableMap<String, ? extends ColumnMeta> keyColumns;

    public KeyValueAggrTableReader(
        TableReader sourceReader,
        ImmutableMap<String, ? extends ColumnMeta> keyColumns,
        ImmutableList<AggrColumn> aggrColumns,
        ColumnMeta havingColumn) {
      super(sourceReader, aggrColumns, havingColumn, keyColumns.size() + aggrColumns.size());
      this.keyColumns = keyColumns;
    }

    private ImmutableList<Object> getKeyValues(Object[] sourceValues) {
      ImmutableList.Builder<Object> builder = ImmutableList.builder();
      for (Map.Entry<String, ? extends ColumnMeta> entry : keyColumns.entrySet()) {
        builder.add(entry.getValue().getValue(sourceValues));
      }
      return builder.build();
    }

    @Override
    protected void aggregate() throws IOException {
      HashMap<ImmutableList<Object>, Object[]> aggregatedValues = new HashMap<>();
      while (!cancelled) {
        Object[] sourceValues = sourceReader.read();
        if (sourceValues == null) {
          break;
        }
        ImmutableList<Object> keyValues = getKeyValues(sourceValues);
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

  public static class GlobalAggrTableReader extends Aggregation {

    public GlobalAggrTableReader(
        TableReader sourceReader,
        ImmutableList<AggrColumn> aggrColumns,
        ColumnMeta havingColumn) {
      super(sourceReader, aggrColumns, havingColumn, aggrColumns.size() + 1);
    }

    @Override
    protected void aggregate() throws IOException {
      Object[] aggrValues = new Object[size];
      int i = 1;
      for (AggrColumn column : aggrColumns) {
        aggrValues[i++] = column.getFunction().init();
      }
      while (!cancelled) {
        Object[] sourceValues = sourceReader.read();
        if (sourceValues == null) {
          break;
        }
        i = 1;
        for (AggrColumn column : aggrColumns) {
          aggrValues[i] = column.getFunction().aggregate(aggrValues[i++], column.getInnerValue(sourceValues));
        }
      }
      i = 1;
      for (AggrColumn column : aggrColumns) {
        aggrValues[i] = column.getFunction().finish(aggrValues[i++]);
      }

      iterator = ImmutableList.of(aggrValues).iterator();
      aggregated = true;
    }
  }
}
