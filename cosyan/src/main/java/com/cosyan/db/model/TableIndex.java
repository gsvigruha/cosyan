package com.cosyan.db.model;

import java.io.IOException;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.ByteTrie.LongIndex;
import com.cosyan.db.index.ByteTrie.StringIndex;
import com.cosyan.db.index.IndexStat.ByteTrieStat;

public abstract class TableIndex {

  public abstract void put(Object key, long fileIndex) throws IOException, IndexException;

  public abstract boolean delete(Object key) throws IOException;

  public abstract long get(Object key) throws IOException;

  public abstract boolean contains(Object key) throws IOException;

  public abstract void commit() throws IOException;

  public abstract void rollback();
  
  public abstract ByteTrieStat stats() throws IOException;

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
    public long get(Object key) throws IOException {
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
      return index.get((Long) key) != null;
    }

    @Override
    public ByteTrieStat stats() throws IOException {
      return index.stats();
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
    public long get(Object key) throws IOException {
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
      return index.get((String) key) != null;
    }
    
    @Override
    public ByteTrieStat stats() throws IOException {
      return index.stats();
    }
  }
}
