package com.cosyan.db.io;

import java.io.IOException;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.model.DataTypes.DataType;

public class Indexes {

  public static interface IndexReader {

    public boolean contains(Object key) throws IOException;

    public long[] get(Object key) throws IOException;
    
    public DataType<?> keyDataType();
  }

  public static interface IndexWriter {

    public abstract void put(Object key, long fileIndex) throws IOException, IndexException;

    public abstract boolean delete(Object key) throws IOException;

  }
}
