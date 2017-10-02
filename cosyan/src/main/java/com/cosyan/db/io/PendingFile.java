package com.cosyan.db.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class PendingFile extends InputStream {

  private final RandomAccessFile file;

  public static final int DEFAULT_BUFFER_SIZE = 65536;

  private final byte[] buffer;
  
  private final long length;
  
  private int pointer;
  
  private long totalPointer;

  public PendingFile(RandomAccessFile file) throws IOException {
    this.file = file;
    this.buffer = new byte[DEFAULT_BUFFER_SIZE];
    this.pointer = DEFAULT_BUFFER_SIZE;
    this.totalPointer = 0;
    file.seek(0);
    length = file.length();
  }

  @Override
  public int read() throws IOException {
    if (totalPointer == length) {
      return -1;
    }
    if (pointer == buffer.length) {
      if (totalPointer + buffer.length <= length) {
        file.read(buffer);  
      } else {
        file.read(buffer, 0, (int) (length - totalPointer));
      }
      pointer = 0;
    }
    totalPointer++;
    return buffer[pointer++] & 0xff;
  }
}
