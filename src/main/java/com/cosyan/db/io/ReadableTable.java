package com.cosyan.db.io;

import java.io.IOException;

import com.cosyan.db.transaction.Resources;

public interface ReadableTable {

  public void init(Resources resources) throws IOException;

  public boolean next() throws IOException;

  public void close() throws IOException;

}
