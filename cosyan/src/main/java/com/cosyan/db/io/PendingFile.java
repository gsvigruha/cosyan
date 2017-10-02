package com.cosyan.db.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class PendingFile extends InputStream {

  private final RandomAccessFile file;

  public PendingFile(RandomAccessFile file) throws IOException {
    this.file = file;
    file.seek(0);
  }

  @Override
  public int read() throws IOException {
    return file.read();
  }
}
