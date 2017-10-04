package com.cosyan.db.model;

import java.io.IOException;

import com.cosyan.db.index.ByteMultiTrie.LongMultiIndex;
import com.cosyan.db.index.ByteMultiTrie.StringMultiIndex;
import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.IndexStat.ByteMultiTrieStat;
import com.cosyan.db.io.Indexes.IndexReader;

public abstract class TableMultiIndex implements IndexReader {
  public abstract void put(Object key, long fileIndex) throws IOException, IndexException;

  public abstract boolean delete(Object key) throws IOException;

  public abstract boolean delete(Object key, long fileIndex) throws IOException;

  public abstract long[] get(Object key) throws IOException;

  public abstract void commit() throws IOException;

  public abstract void rollback();

  public abstract boolean contains(Object key) throws IOException;

  public abstract ByteMultiTrieStat stats() throws IOException;

  private boolean valid = true;

  public void invalidate() {
    valid = false;
  }

  public boolean isValid() {
    return valid;
  }

  public static class LongTableMultiIndex extends TableMultiIndex {

    private LongMultiIndex index;

    public LongTableMultiIndex(LongMultiIndex index) {
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
    public boolean delete(Object key, long fileIndex) throws IOException {
      return index.delete((Long) key, fileIndex);
    }

    @Override
    public long[] get(Object key) throws IOException {
      return index.get((Long) key);
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
      return index.get((Long) key).length > 0;
    }

    @Override
    public ByteMultiTrieStat stats() throws IOException {
      return index.stats();
    }
  }

  public static class StringTableMultiIndex extends TableMultiIndex {

    private StringMultiIndex index;

    public StringTableMultiIndex(StringMultiIndex index) {
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
    public boolean delete(Object key, long fileIndex) throws IOException {
      return index.delete((String) key, fileIndex);
    }

    @Override
    public long[] get(Object key) throws IOException {
      return index.get((String) key);
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
      return index.get((String) key).length > 0;
    }

    @Override
    public ByteMultiTrieStat stats() throws IOException {
      return index.stats();
    }
  }
}
