package com.cosyan.db.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.RecordReader.Record;
import com.cosyan.db.logic.PredicateHelper.VariableEquals;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.ColumnMeta.OrderColumn;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.SourceValues;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
public abstract class TableReader implements TableIO {

  protected boolean cancelled = false;

  public abstract SourceValues read() throws IOException;

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static abstract class ExposedTableReader extends TableReader {

    protected final ImmutableMap<String, ? extends ColumnMeta> columns;

    public ImmutableMap<String, Object> readColumns() throws IOException {
      SourceValues values = read();
      if (values.isEmpty()) {
        return null;
      }
      ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
      int i = 0;
      for (String columnName : columns.keySet()) {
        builder.put(columnName, values.sourceValue(i++));
      }
      return builder.build();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = true)
  public static abstract class SeekableTableReader extends ExposedTableReader {
    public SeekableTableReader(List<BasicColumn> columns) {
      super(MaterializedTableMeta.columnsMap(columns));
    }

    public abstract void seek(long position) throws IOException;

    public abstract IndexReader indexReader(Ident ident);
  }

  public static class MaterializedTableReader extends SeekableTableReader {

    private final RecordReader reader;
    private final RAFBufferedInputStream bufferedRAF;
    private final ImmutableMap<String, IndexReader> indexes;

    public MaterializedTableReader(
        RandomAccessFile raf,
        ImmutableList<BasicColumn> columns,
        ImmutableMap<String, IndexReader> indexes) throws IOException {
      super(columns);
      this.indexes = indexes;
      this.bufferedRAF = new RAFBufferedInputStream(raf);
      this.reader = new RecordReader(columns, bufferedRAF);
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
    public SourceValues read() throws IOException {
      Record record = reader.read();
      if (record == RecordReader.EMPTY) {
        close();
      }
        // TODO
      return record.sourceValues();
    }

    @Override
    public IndexReader indexReader(Ident ident) {
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
    public SourceValues read() throws IOException {
      SourceValues sourceValues = sourceReader.read();
      if (sourceValues.isEmpty()) {
        return sourceValues;
      } else {
        Object[] values = new Object[columns.size()];
        int i = 0;
        for (Map.Entry<String, ? extends ColumnMeta> entry : columns.entrySet()) {
          values[i++] = entry.getValue().getValue(sourceValues);
        }
        return SourceValues.of(values);
      }
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
    public SourceValues read() throws IOException {
      SourceValues values = SourceValues.EMPTY;
      do {
        SourceValues sourceValues = sourceReader.read();
        if (sourceValues.isEmpty()) {
          return sourceValues;
        } else {
          if (!(boolean) whereColumn.getValue(sourceValues)) {
            values = SourceValues.EMPTY;
          } else {
            values = sourceValues;
          }
        }
      } while (values.isEmpty() && !cancelled);
      return values;
    }
  }

  public static class IndexFilteredTableReader extends ExposedTableReader {

    private final SeekableTableReader sourceReader;
    private final ColumnMeta whereColumn;
    private final VariableEquals clause;
    private final IndexReader index;

    private long[] positions;
    private int pointer;

    public IndexFilteredTableReader(
        SeekableTableReader sourceReader,
        ColumnMeta whereColumn,
        VariableEquals clause) {
      super(sourceReader.columns);
      this.sourceReader = sourceReader;
      this.whereColumn = whereColumn;
      this.clause = clause;
      this.index = sourceReader.indexReader(clause.getIdent());
    }

    @Override
    public void close() throws IOException {
      sourceReader.close();
    }

    @Override
    public SourceValues read() throws IOException {
      if (positions == null) {
        readPositions();
      }
      SourceValues values = SourceValues.EMPTY;
      do {
        if (pointer < positions.length) {
          sourceReader.seek(positions[pointer]);
          SourceValues sourceValues = sourceReader.read();
          if (sourceValues.isEmpty()) {
            return sourceValues;
          } else {
            if (!(boolean) whereColumn.getValue(sourceValues)) {
              values = SourceValues.EMPTY;
            } else {
              values = sourceValues;
            }
          }
          pointer++;
        } else {
          return SourceValues.EMPTY;
        }
      } while (values.isEmpty() && !cancelled);
      return values;
    }

    private void readPositions() throws IOException {
      positions = index.get(clause.getValue());
      pointer = 0;
    }
  }

  public static class SortedTableReader extends ExposedTableReader {

    private final ExposedTableReader sourceReader;
    private final ImmutableList<OrderColumn> orderColumns;
    private boolean sorted;
    private Iterator<SourceValues> iterator;

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
    public SourceValues read() throws IOException {
      if (!sorted) {
        sort();
      }
      if (!iterator.hasNext()) {
        return SourceValues.EMPTY;
      }
      return iterator.next();
    }

    private void sort() throws IOException {
      TreeMap<ImmutableList<Object>, SourceValues> values = new TreeMap<>(new Comparator<ImmutableList<Object>>() {
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
        SourceValues sourceValues = sourceReader.read();
        if (sourceValues.isEmpty()) {
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

  public static class DistinctTableReader extends ExposedTableReader {

    private final ExposedTableReader sourceReader;
    private boolean distinct;
    private Iterator<SourceValues> iterator;

    public DistinctTableReader(
        ExposedTableReader sourceReader) {
      super(sourceReader.columns);
      this.sourceReader = sourceReader;
      this.distinct = false;
    }

    @Override
    public void close() throws IOException {
      sourceReader.close();
    }

    @Override
    public SourceValues read() throws IOException {
      if (!distinct) {
        distinct();
      }
      if (!iterator.hasNext()) {
        return SourceValues.EMPTY;
      }
      return iterator.next();
    }

    private void distinct() throws IOException {
      LinkedHashSet<ImmutableList<Object>> values = new LinkedHashSet<>();
      while (!cancelled) {
        SourceValues sourceValues = sourceReader.read();
        if (sourceValues.isEmpty()) {
          break;
        }
        values.add(sourceValues.toList());
      }
      iterator = values.stream().map(list -> SourceValues.of(list.toArray())).iterator();
      distinct = true;
    }
  }
}
