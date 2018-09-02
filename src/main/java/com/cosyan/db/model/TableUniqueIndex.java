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
package com.cosyan.db.model;

import java.io.IOException;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.IDIndex;
import com.cosyan.db.index.IndexStat.ByteTrieStat;
import com.cosyan.db.index.LongLeafTries.DoubleIndex;
import com.cosyan.db.index.LongLeafTries.LongIndex;
import com.cosyan.db.index.LongLeafTries.StringIndex;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.io.Indexes.IndexWriter;
import com.cosyan.db.model.DataTypes.DataType;

public abstract class TableUniqueIndex implements IndexReader, IndexWriter {

  public abstract void put(Object key, long fileIndex) throws IOException, IndexException;

  public abstract boolean delete(Object key) throws IOException;

  public abstract long get0(Object key) throws IOException;

  public abstract void commit() throws IOException;

  public abstract void rollback();

  public abstract ByteTrieStat stats() throws IOException;

  public abstract void drop() throws IOException;

  private boolean valid = true;

  public void invalidate() {
    valid = false;
  }

  public boolean isValid() {
    return valid;
  }

  public static class LongTableIndex extends TableUniqueIndex {

    private final LongIndex index;

    public LongTableIndex(LongIndex index) {
      this.index = index;
    }

    @Override
    public void put(Object key, long fileIndex) throws IOException, IndexException {
      index.put((Long) key, fileIndex);
    }

    @Override
    public boolean delete(Object key) throws IOException {
      return index.delete((Long) key);
    }

    @Override
    public long[] get(Object key) throws IOException {
      long filePointer = get0(key);
      if (filePointer < 0) {
        return new long[0];
      } else {
        return new long[] { filePointer };
      }
    }

    @Override
    public long get0(Object key) throws IOException {
      Long filePointer = index.get((Long) key);
      if (filePointer == null) {
        return -1;
      } else {
        return filePointer;
      }
    }

    @Override
    public void commit() throws IOException {
      index.commit();
    }

    @Override
    public void rollback() {
      index.rollback();
    }

    @Override
    public boolean contains(Object key) throws IOException {
      return index.get((Long) key) != null;
    }

    @Override
    public ByteTrieStat stats() throws IOException {
      return index.stats();
    }

    @Override
    public void drop() throws IOException {
      index.drop();
    }

    @Override
    public DataType<?> keyDataType() {
      return DataTypes.LongType;
    }
  }

  public static class StringTableIndex extends TableUniqueIndex {

    private final StringIndex index;

    public StringTableIndex(StringIndex index) {
      this.index = index;
    }

    @Override
    public void put(Object key, long fileIndex) throws IOException, IndexException {
      index.put((String) key, fileIndex);
    }

    @Override
    public boolean delete(Object key) throws IOException {
      return index.delete((String) key);
    }

    @Override
    public long[] get(Object key) throws IOException {
      long filePointer = get0(key);
      if (filePointer < 0) {
        return new long[0];
      } else {
        return new long[] { filePointer };
      }
    }

    @Override
    public long get0(Object key) throws IOException {
      Long filePointer = index.get((String) key);
      if (filePointer == null) {
        return -1;
      } else {
        return filePointer;
      }
    }

    @Override
    public void commit() throws IOException {
      index.commit();
    }

    @Override
    public void rollback() {
      index.rollback();
    }

    @Override
    public boolean contains(Object key) throws IOException {
      return index.get((String) key) != null;
    }

    @Override
    public ByteTrieStat stats() throws IOException {
      return index.stats();
    }

    @Override
    public void drop() throws IOException {
      index.drop();
    }

    @Override
    public DataType<?> keyDataType() {
      return DataTypes.StringType;
    }
  }

  public static class DoubleTableIndex extends TableUniqueIndex {

    private final DoubleIndex index;

    public DoubleTableIndex(DoubleIndex index) {
      this.index = index;
    }

    @Override
    public void put(Object key, long fileIndex) throws IOException, IndexException {
      index.put((Double) key, fileIndex);
    }

    @Override
    public boolean delete(Object key) throws IOException {
      return index.delete((Double) key);
    }

    @Override
    public long[] get(Object key) throws IOException {
      long filePointer = get0(key);
      if (filePointer < 0) {
        return new long[0];
      } else {
        return new long[] { filePointer };
      }
    }

    @Override
    public long get0(Object key) throws IOException {
      Long filePointer = index.get((Double) key);
      if (filePointer == null) {
        return -1;
      } else {
        return filePointer;
      }
    }

    @Override
    public void commit() throws IOException {
      index.commit();
    }

    @Override
    public void rollback() {
      index.rollback();
    }

    @Override
    public boolean contains(Object key) throws IOException {
      return index.get((Double) key) != null;
    }

    @Override
    public ByteTrieStat stats() throws IOException {
      return index.stats();
    }

    @Override
    public void drop() throws IOException {
      index.drop();
    }

    @Override
    public DataType<?> keyDataType() {
      return DataTypes.DoubleType;
    }
  }

  public static class IDTableIndex extends TableUniqueIndex {

    private final IDIndex index;

    public IDTableIndex(IDIndex index) {
      this.index = index;
    }

    @Override
    public void put(Object key, long fileIndex) throws IOException, IndexException {
      index.put((Long) key, fileIndex);
    }

    @Override
    public boolean delete(Object key) throws IOException {
      return index.delete((Long) key);
    }

    @Override
    public long[] get(Object key) throws IOException {
      long filePointer = get0(key);
      if (filePointer < 0) {
        return new long[0];
      } else {
        return new long[] { filePointer };
      }
    }

    @Override
    public long get0(Object key) throws IOException {
      Long filePointer = index.get((Long) key);
      if (filePointer == null) {
        return -1;
      } else {
        return filePointer;
      }
    }

    @Override
    public void commit() throws IOException {
      index.commit();
    }

    @Override
    public void rollback() {
      index.rollback();
    }

    @Override
    public boolean contains(Object key) throws IOException {
      return index.get((Long) key) != null;
    }

    @Override
    public ByteTrieStat stats() throws IOException {
      return index.stats();
    }

    @Override
    public void drop() throws IOException {
      index.drop();
    }

    public long getLastID() {
      return index.getLastID();
    }

    @Override
    public DataType<?> keyDataType() {
      return DataTypes.IDType;
    }
  }
}
