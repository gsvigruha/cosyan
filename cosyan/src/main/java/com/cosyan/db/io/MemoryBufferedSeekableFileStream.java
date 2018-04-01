package com.cosyan.db.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class MemoryBufferedSeekableFileStream extends SeekableInputStream implements SeekableOutputStream {

  public static final int DEFAULT_BUFFER_SIZE = 65536;

  private final RandomAccessFile file;

  private byte[] buffer;

  private int pointer;

  public MemoryBufferedSeekableFileStream(RandomAccessFile file) throws IOException {
    this.file = file;
    long fileSize = file.length();
    if (fileSize > Integer.MAX_VALUE) {
      throw new IOException();
    }
    this.buffer = new byte[(int) fileSize];
    file.seek(0);
    file.read(buffer);
    this.pointer = 0;
  }

  @Override
  public int read() throws IOException {
    if (pointer == buffer.length) {
      return -1;
    }
    return buffer[pointer++] & 0xff;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  @Override
  public long length() {
    return buffer.length;
  }

  @Override
  public void seek(long position) throws IOException {
    pointer = (int) position;
  }

  public void reset() throws IOException {
    pointer = 0;
  }

  @Override
  public void write(long position, byte[] value) throws IOException {
    file.seek(position);
    file.write(value);
    if (position + value.length < buffer.length) {
      System.arraycopy(value, 0, buffer, (int) position, value.length);
      pointer = (int) (position + value.length);
    } else {
      long fileSize = position + value.length;
      byte[] newBuffer = new byte[(int) fileSize];
      System.arraycopy(this.buffer, 0, newBuffer, 0, this.buffer.length);
      System.arraycopy(value, 0, newBuffer, (int) position, value.length);
      this.buffer = newBuffer;
    }
  }

  @Override
  public FileChannel getChannel() {
    return file.getChannel();
  }
}
