package com.cosyan.db.io;

import java.io.IOException;

public class Indexes {

  public static interface IndexReader {

    public boolean contains(Object key) throws IOException;
    
    public long[] get(Object key) throws IOException;
  }
}
