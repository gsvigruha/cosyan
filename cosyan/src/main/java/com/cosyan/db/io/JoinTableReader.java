package com.cosyan.db.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.SourceValues;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;

public class JoinTableReader {
  public static class HashJoinTableReader extends ExposedTableReader {

    private final ExposedTableReader mainTableReader;
    private final ExposedTableReader joinTableReader;
    private final ImmutableList<ColumnMeta> mainTableJoinColumns;
    private final ImmutableList<ColumnMeta> joinTableJoinColumns;
    private final boolean mainTableFirst;
    private final boolean innerJoin;

    private boolean joined;
    private LinkedListMultimap<ImmutableList<Object>, Object[]> joinValues;
    private Iterator<Object[]> joinValuesForCurrentKey;
    private SourceValues mainTableValues;

    public HashJoinTableReader(
        ExposedTableReader mainTableReader,
        ExposedTableReader joinTableReader,
        ImmutableList<ColumnMeta> mainTableJoinColumns,
        ImmutableList<ColumnMeta> joinTableJoinColumns,
        boolean mainTableFirst,
        boolean innerJoin) {
      super(null); // TODO fix design here
      this.mainTableReader = mainTableReader;
      this.joinTableReader = joinTableReader;
      this.mainTableJoinColumns = mainTableJoinColumns;
      this.joinTableJoinColumns = joinTableJoinColumns;
      this.mainTableFirst = mainTableFirst;
      this.innerJoin = innerJoin;

      this.joined = false;
    }

    @Override
    public void close() throws IOException {
      mainTableReader.close();
      joinTableReader.close();
    }

    @Override
    public SourceValues read() throws IOException {
      if (!joined) {
        join();
      }
      SourceValues result = SourceValues.EMPTY;
      do {
        while (joinValuesForCurrentKey == null || !joinValuesForCurrentKey.hasNext()) {
          List<Object[]> values = null;
          mainTableValues = mainTableReader.read();
          if (mainTableValues.isEmpty()) {
            return mainTableValues;
          }
          ImmutableList.Builder<Object> builder = ImmutableList.builder();
          for (ColumnMeta column : mainTableJoinColumns) {
            Object key = column.getValue(mainTableValues);
            builder.add(key);
          }
          values = joinValues.get(builder.build());
          if (values != null && !values.isEmpty()) {
            joinValuesForCurrentKey = values.iterator();
          } else if (!innerJoin) {
            Object[] nullValues = new Object[joinTableReader.columns.size()];
            Arrays.fill(nullValues, DataTypes.NULL);
            joinValuesForCurrentKey = ImmutableList.of(nullValues).iterator();
          }
        }

        result = SourceValues.of(match(mainTableValues.toArray(), joinValuesForCurrentKey.next()));
      } while (result.isEmpty());
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
        SourceValues sourceValues = joinTableReader.read();
        if (sourceValues.isEmpty()) {
          break;
        }
        ImmutableList.Builder<Object> builder = ImmutableList.builder();
        for (ColumnMeta column : joinTableJoinColumns) {
          Object key = column.getValue(sourceValues);
          builder.add(key);
        }
        joinValues.put(builder.build(), sourceValues.toArray());
      }
      joined = true;
    }
  }
}
