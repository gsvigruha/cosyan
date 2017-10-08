package com.cosyan.db.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.RecordReader.Record;
import com.cosyan.db.logic.WhereClause.IndexLookup;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public abstract class TableReader implements TableIO {

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

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static abstract class SeekableTableReader extends ExposedTableReader {
    public SeekableTableReader(ImmutableMap<String, ? extends ColumnMeta> columns) {
      super(columns);
    }

    public abstract void seek(long position) throws IOException;

    public abstract IndexReader getIndex(Ident ident);
  }

  public static class MaterializedTableReader extends SeekableTableReader {

    private final RecordReader reader;
    private final RAFBufferedInputStream bufferedRAF;
    private final ImmutableMap<String, IndexReader> indexes;

    public MaterializedTableReader(
        RandomAccessFile raf,
        ImmutableMap<String, ? extends ColumnMeta> columns,
        ImmutableMap<String, IndexReader> indexes) throws IOException {
      super(columns);
      this.indexes = indexes;
      this.bufferedRAF = new RAFBufferedInputStream(raf);
      this.reader = new RecordReader(
          columns.values().asList(),
          bufferedRAF);
    }

    @Override
    public void close() throws IOException {
      bufferedRAF.close();
    }

    @Override
    public void seek(long position) throws IOException {
      bufferedRAF.seek(position);
    }

    @Override
    public Object[] read() throws IOException {
      Record record = reader.read();
      if (record == RecordReader.EMPTY) {
        close();
      }
      return record.getValues();
    }

    @Override
    public IndexReader getIndex(Ident ident) {
      return indexes.get(ident.getString());
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

  public static class IndexFilteredTableReader extends ExposedTableReader {

    private final SeekableTableReader sourceReader;
    private final ColumnMeta whereColumn;
    private final IndexLookup indexLookup;
    private final IndexReader index;

    private long[] positions;
    private int pointer;

    public IndexFilteredTableReader(
        SeekableTableReader sourceReader,
        ColumnMeta whereColumn,
        IndexLookup indexLookup) {
      super(sourceReader.columns);
      this.sourceReader = sourceReader;
      this.whereColumn = whereColumn;
      this.indexLookup = indexLookup;
      this.index = sourceReader.getIndex(indexLookup.getIdent());
    }

    @Override
    public void close() throws IOException {
      sourceReader.close();
    }

    @Override
    public Object[] read() throws IOException {
      if (positions == null) {
        readPositions();
      }
      Object[] values = null;
      do {
        if (pointer < positions.length) {
          sourceReader.seek(positions[pointer]);
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
          pointer++;
        } else {
          return null;
        }
      } while (values == null && !cancelled);
      return values;
    }

    private void readPositions() throws IOException {
      positions = index.get(indexLookup.getValue());
      pointer = 0;
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
}
