package com.cosyan.db.io;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.RecordProvider.Record;
import com.cosyan.db.io.RecordProvider.RecordReader;
import com.cosyan.db.io.RecordProvider.SeekableRecordReader;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.TableUniqueIndex;
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

    protected final MaterializedTable tableMeta;

    public SeekableTableReader(
        MaterializedTable tableMeta) {
      this.tableMeta = tableMeta;
    }

    public abstract void close() throws IOException;

    public abstract Record get(long position) throws IOException;

    public abstract Record get(Object key, Resources resources) throws IOException;

    public abstract IterableTableReader iterableReader() throws IOException;

    public TableUniqueIndex getPrimaryKeyIndex() {
      return (TableUniqueIndex) getIndex(tableMeta.primaryKey().get().getColumn().getName());
    }

    public abstract IndexReader getIndex(String name);
  }

  public static class MaterializedTableReader extends SeekableTableReader {

    private final SeekableRecordReader reader;
    private final SeekableInputStream fileReader;
    private final ImmutableMap<String, IndexReader> indexes;
    private final String fileName;
    private final ImmutableList<BasicColumn> columns;

    public MaterializedTableReader(
        MaterializedTable tableMeta,
        String fileName,
        SeekableInputStream fileReader,
        ImmutableList<BasicColumn> columns,
        ImmutableMap<String, IndexReader> indexes) throws IOException {
      super(tableMeta);
      this.indexes = indexes;
      this.fileReader = fileReader;
      this.reader = new SeekableRecordReader(columns, fileReader);
      this.fileName = fileName;
      this.columns = columns;
    }

    @Override
    public void close() throws IOException {
      fileReader.close();
    }

    @Override
    public Record get(long position) throws IOException {
      reader.seek(position);
      return reader.read();
    }

    @Override
    public Record get(Object key, Resources resources) throws IOException {
      TableUniqueIndex index = resources.getPrimaryKeyIndex(tableMeta.tableName());
      long filePointer = index.get0(key);
      return get(filePointer);
    }

    @Override
    public IterableTableReader iterableReader() throws IOException {
      RecordReader reader = new RecordReader(columns, new BufferedInputStream(new FileInputStream(fileName)));
      return new IterableTableReader() {

        @Override
        public Object[] next() throws IOException {
          return reader.read().getValues();
        }

        @Override
        public void close() throws IOException {
          reader.close();
        }
      };
    }

    @Override
    public IndexReader getIndex(String name) {
      return indexes.get(name);
    }
  }

  public static abstract class MultiFilteredTableReader extends IterableTableReader implements RecordProvider {

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
      return read().getValues();
    }

    @Override
    public Record read() throws IOException {
      if (positions == null) {
        readPositions();
        pointer = 0;
      }
      Record record = RecordReader.EMPTY;
      boolean keepGoing = false;
      do {
        if (pointer < positions.length) {
          record = sourceReader.get(positions[pointer]);
          if (record == RecordReader.EMPTY) {
            return record;
          } else {
            if (!(boolean) whereColumn.value(record.getValues(), resources)) {
              keepGoing = true;
            }
          }
          pointer++;
        } else {
          return RecordReader.EMPTY;
        }
      } while (keepGoing && !cancelled);
      return record;
    }

    protected abstract void readPositions() throws IOException;

    @Override
    public void close() throws IOException {
      // SeekableTableReader should not be closed manually.
    }
  }
}
