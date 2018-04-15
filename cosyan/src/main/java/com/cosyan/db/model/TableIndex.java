package com.cosyan.db.model;

import java.io.IOException;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.ByteTrie.LongIndex;
import com.cosyan.db.index.ByteTrie.StringIndex;
import com.cosyan.db.index.IDIndex;
import com.cosyan.db.index.IndexStat.ByteTrieStat;
import com.cosyan.db.io.Indexes.IndexReader;

public abstract class TableIndex implements IndexReader {

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

  public static class LongTableIndex extends TableIndex {

    private LongIndex index;

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
  }

  public static class StringTableIndex extends TableIndex {

    private StringIndex index;

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
  }

  public static class IDTableIndex extends TableIndex {

    private IDIndex index;

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
      // TODO
      return null;
    }

    @Override
    public void drop() throws IOException {
      index.drop();
    }

    public long getLastID() {
      return index.getLastID();
    }
  }
}
