package com.cosyan.db.io;

import java.io.IOException;
import java.io.RandomAccessFile;

public class RAFBufferedInputStream extends SeekableInputStream {

  public static final int DEFAULT_BUFFER_SIZE = 65536;

  private final RandomAccessFile file;

  private final byte[] buffer;

  private final long length;

  private int pointer;

  private long totalPointer;

  public RAFBufferedInputStream(RandomAccessFile file) throws IOException {
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
      // End of buffer, reload.
      if (totalPointer + buffer.length <= length) {
        // Read the whole buffer.
        file.read(buffer);
      } else {
        // Read the end of the file.
        file.read(buffer, 0, (int) (length - totalPointer));
      }
      pointer = 0;
    }
    totalPointer++;
    return buffer[pointer++] & 0xff;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public void seek(long position) throws IOException {
    pointer = buffer.length;
    totalPointer = position;
    file.seek(position);
  }

  public Object position() {
    return totalPointer;
  }
}
