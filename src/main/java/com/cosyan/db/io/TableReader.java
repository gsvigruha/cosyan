/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.io;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.RecordProvider.Record;
import com.cosyan.db.io.RecordProvider.RecordReader;
import com.cosyan.db.io.RecordProvider.SeekableRecordReader;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.View;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.ColumnMeta;
import com.cosyan.db.model.Keys.PrimaryKey;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.TableContext;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.model.TableUniqueIndex;
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

    protected AtomicBoolean cancelled = new AtomicBoolean(false);

    public abstract Object[] next() throws IOException;

    public abstract void close() throws IOException;

    public void cancel() {
      cancelled.set(true);
    }
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

    protected boolean cancelled = false;

    public abstract void close() throws IOException;

    public abstract Record get(long position) throws IOException;

    public abstract Record get(Object key, Resources resources) throws IOException;

    public abstract IterableTableReader iterableReader(Resources resources) throws IOException;

    public abstract TableUniqueIndex getPrimaryKeyIndex();

    public abstract IndexReader getIndex(String name);

    public void checkRule(Rule rule, Resources resources) throws IOException, RuleException {
      IterableTableReader reader = iterableReader(resources);
      Object[] values;
      try {
        while ((values = reader.next()) != null && !cancelled) {
          if (!rule.check(resources, values)) {
            throw new RuleException(String.format("Constraint check %s failed.", rule.getName()));
          }
        }
      } finally {
        reader.close();
      }
    }

    public void cancel() {
      cancelled = true;
    }
  }

  public static class MaterializedTableReader extends SeekableTableReader {

    private final SeekableRecordReader reader;
    private final SeekableInputStream fileReader;
    private final Map<String, IndexReader> indexes;
    private final String fileName;
    private final ImmutableList<BasicColumn> columns;
    private final Optional<PrimaryKey> primaryKey;

    private Object cachedKey;
    private Record cachedRecord;

    public MaterializedTableReader(MaterializedTable tableMeta, String fileName,
        SeekableInputStream fileReader, ImmutableList<BasicColumn> columns,
        Map<String, IndexReader> indexes) throws IOException {
      this.indexes = indexes;
      this.fileReader = fileReader;
      this.reader = new SeekableRecordReader(columns, fileReader);
      this.fileName = fileName;
      this.columns = columns;
      this.primaryKey = tableMeta.primaryKey();
    }

    @Override
    public TableUniqueIndex getPrimaryKeyIndex() {
      return (TableUniqueIndex) getIndex(primaryKey.get().getColumn().getName());
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
      if (cachedKey != null && cachedKey.equals(key)) {
        return cachedRecord;
      }
      TableUniqueIndex index = getPrimaryKeyIndex();
      long filePointer = index.get0(key);
      Record record = get(filePointer);
      cachedKey = key;
      cachedRecord = record;
      return record;
    }

    @Override
    public IterableTableReader iterableReader(Resources resources) throws IOException {
      RecordReader reader = recordReader();
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

    protected RecordReader recordReader() throws IOException {
      return new RecordReader(columns, new BufferedInputStream(new FileInputStream(fileName)));
    }
  }

  public static class ViewReader extends SeekableTableReader {

    private final View view;

    public ViewReader(View view) {
      this.view = view;
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public Record get(long position) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Record get(Object key, Resources resources) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public IterableTableReader iterableReader(Resources resources) throws IOException {
      return view.table().reader(resources, TableContext.EMPTY);
    }

    @Override
    public TableUniqueIndex getPrimaryKeyIndex() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public IndexReader getIndex(String name) {
      // TODO Auto-generated method stub
      return null;
    }
  }

  public static abstract class MultiFilteredTableReader extends IterableTableReader
      implements RecordProvider {

    protected final SeekableTableReader sourceReader;
    protected final ColumnMeta whereColumn;
    private final Resources resources;

    private long[] positions;
    private int pointer;
    private boolean cancelled;

    public MultiFilteredTableReader(SeekableTableReader sourceReader, ColumnMeta whereColumn,
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
        positions = readPositions();
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
            if (!(boolean) whereColumn.value(record.getValues(), resources, TableContext.EMPTY)) {
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

    protected abstract long[] readPositions() throws IOException;

    @Override
    public void close() throws IOException {
      // SeekableTableReader should not be closed manually.
    }
  }
}
