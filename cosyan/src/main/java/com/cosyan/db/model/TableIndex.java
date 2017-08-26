package com.cosyan.db.model;

import java.io.IOException;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.ByteTrie.LongIndex;
import com.cosyan.db.index.ByteTrie.StringIndex;

public abstract class TableIndex {

  public abstract void put(Object key, long fileIndex) throws IOException, IndexException;

  public abstract boolean delete(Object key) throws IOException;

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
  }
}
