package com.cosyan.db.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.cosyan.db.model.Aggregators.Aggregator;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.AggrColumn;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public abstract class AggrReader extends TableReader {

  protected final TableReader sourceReader;
  protected final ColumnMeta havingColumn;
  protected final ImmutableList<AggrColumn> aggrColumns;
  protected final int size;

  protected Iterator<Object[]> iterator;
  protected boolean aggregated;

  protected AggrReader(
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

  public static class KeyValueAggrTableReader extends AggrReader {

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
      HashMap<ImmutableList<Object>, Aggregator<?, ?>[]> aggregatedValues = new HashMap<>();
      while (!cancelled) {
        Object[] sourceValues = sourceReader.read();
        if (sourceValues == null) {
          break;
        }
        ImmutableList<Object> keyValues = getKeyValues(sourceValues);
        if (!aggregatedValues.containsKey(keyValues)) {
          Aggregator<?, ?>[] aggrValues = new Aggregator[aggrColumns.size()];
          int i = 0;
          for (AggrColumn column : aggrColumns) {
            aggrValues[i++] = column.getFunction().create();
          }
          aggregatedValues.put(keyValues, aggrValues);
        }
        Aggregator<?, ?>[] aggrValues = aggregatedValues.get(keyValues);
        int i = 0;
        for (AggrColumn column : aggrColumns) {
          aggrValues[i++].add(column.getInnerValue(sourceValues));
        }
      }
      final Iterator<Entry<ImmutableList<Object>, Aggregator<?, ?>[]>> innerIterator = aggregatedValues.entrySet()
          .iterator();
      iterator = new Iterator<Object[]>() {

        @Override
        public boolean hasNext() {
          return innerIterator.hasNext();
        }

        @Override
        public Object[] next() {
          Object[] result = new Object[size];
          Entry<ImmutableList<Object>, Aggregator<?, ?>[]> item = innerIterator.next();
          Object[] keys = item.getKey().toArray();
          System.arraycopy(keys, 0, result, 0, keys.length);
          for (int i = 0; i < item.getValue().length; i++) {
            result[keys.length + i] = item.getValue()[i].finish();
          }
          return result;
        }
      };
      aggregated = true;
    }
  }

  public static class GlobalAggrTableReader extends AggrReader {

    public GlobalAggrTableReader(
        TableReader sourceReader,
        ImmutableList<AggrColumn> aggrColumns,
        ColumnMeta havingColumn) {
      super(sourceReader, aggrColumns, havingColumn, aggrColumns.size() + 1);
    }

    @Override
    protected void aggregate() throws IOException {
      Aggregator<?, ?>[] aggrValues = new Aggregator[size];
      int i = 1;
      for (AggrColumn column : aggrColumns) {
        aggrValues[i++] = column.getFunction().create();
      }
      while (!cancelled) {
        Object[] sourceValues = sourceReader.read();
        if (sourceValues == null) {
          break;
        }
        i = 1;
        for (AggrColumn column : aggrColumns) {
          aggrValues[i++].add(column.getInnerValue(sourceValues));
        }
      }
      Object[] result = new Object[size];
      for (int j = 0; j < aggrColumns.size(); j++) {
        result[j + 1] = aggrValues[j + 1].finish();
      }

      iterator = ImmutableList.of(result).iterator();
      aggregated = true;
    }
  }
}
