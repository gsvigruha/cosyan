package com.cosyan.db.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.RecordReader.Record;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.Data;

@Data
public abstract class TableReader implements TableIO {

  @Data
  public static class ExposedTableReader {

    private final ExposedTableMeta tableMeta;
    private final IterableTableReader reader;

    public ImmutableMap<String, Object> readColumns() throws IOException {
      Object[] values = reader.next();
      if (values == null) {
        return null;
      }
      ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
      int i = 0;
      for (String columnName : tableMeta.columnNames()) {
        builder.put(columnName, values[i++]);
      }
      return builder.build();
    }
  }

  public static abstract class IterableTableReader {
    public abstract Object[] next() throws IOException;

    public abstract void close() throws IOException;
  }

  public static abstract class DerivedIterableTableReader extends IterableTableReader {

    protected final IterableTableReader sourceReader;

    public DerivedIterableTableReader(IterableTableReader sourceReader) {
      this.sourceReader = sourceReader;
    }

    @Override
    public void close() throws IOException {
      sourceReader.close();
    }
  }

  public static abstract class SeekableTableReader implements TableIO {

    private final MaterializedTableMeta tableMeta;

    public SeekableTableReader(
        MaterializedTableMeta tableMeta) {
      this.tableMeta = tableMeta;
    }

    public abstract void close() throws IOException;

    public abstract Record get(long position) throws IOException;

    public abstract IterableTableReader iterableReader(Resources resources) throws IOException;

    public TableIndex getPrimaryKeyIndex() {
      return (TableIndex) getIndex(tableMeta.primaryKey().get().getColumn().getName());
    }

    public abstract IndexReader getIndex(String name);
  }

  public static class MaterializedTableReader extends SeekableTableReader {

    private final RecordReader reader;
    private final RAFBufferedInputStream bufferedRAF;
    private final ImmutableMap<String, IndexReader> indexes;
    private final String fileName;
    private final ImmutableList<BasicColumn> columns;

    public MaterializedTableReader(
        MaterializedTableMeta tableMeta,
        String fileName,
        ImmutableList<BasicColumn> columns,
        ImmutableMap<String, IndexReader> indexes) throws IOException {
      super(tableMeta);
      this.indexes = indexes;
      this.bufferedRAF = new RAFBufferedInputStream(new RandomAccessFile(fileName, "r"));
      this.reader = new RecordReader(columns, bufferedRAF);
      this.fileName = fileName;
      this.columns = columns;
    }

    @Override
    public void close() throws IOException {
      bufferedRAF.close();
    }

    @Override
    public Record get(long position) throws IOException {
      reader.seek(position);
      return reader.read();
    }

    @Override
    public IterableTableReader iterableReader(Resources resources) throws IOException {
      RAFBufferedInputStream rafReader = new RAFBufferedInputStream(new RandomAccessFile(fileName, "r"));
      RecordReader reader = new RecordReader(columns, rafReader);
      return new IterableTableReader() {

        @Override
        public Object[] next() throws IOException {
          return reader.read().getValues();
        }

        @Override
        public void close() throws IOException {
          rafReader.close();
        }
      };
    }

    @Override
    public IndexReader getIndex(String name) {
      return indexes.get(name);
    }
  }

  public static abstract class MultiFilteredTableReader extends IterableTableReader {

    protected final SeekableTableReader sourceReader;
    protected final ColumnMeta whereColumn;
    private final Resources resources;

    protected long[] positions;
    private int pointer;
    private boolean cancelled;

    public MultiFilteredTableReader(
        SeekableTableReader sourceReader,
        ColumnMeta whereColumn,
        Resources resources) {
      this.sourceReader = sourceReader;
      this.whereColumn = whereColumn;
      this.resources = resources;
    }

    public void reset() {
      positions = null;
    }

    @Override
    public Object[] next() throws IOException {
      if (positions == null) {
        readPositions();
        pointer = 0;
      }
      Object[] values = null;
      do {
        if (pointer < positions.length) {
          values = sourceReader.get(positions[pointer]).getValues();
          if (values == null) {
            return null;
          } else {
            if (!(boolean) whereColumn.getValue(values, resources)) {
              values = null;
            }
          }
          pointer++;
        } else {
          return null;
        }
      } while (values == null && !cancelled);
      return values;
    }

    protected abstract void readPositions() throws IOException;

    @Override
    public void close() throws IOException {
      // SeekableTableReader should not be closed manually.
    }
  }
}
