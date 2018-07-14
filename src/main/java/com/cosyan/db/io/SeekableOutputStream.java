package com.cosyan.db.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface SeekableOutputStream {

  public void write(long position, byte[] value) throws IOException;

  public FileChannel getChannel();

  public void close() throws IOException;

  public static class RAFSeekableOutputStream implements SeekableOutputStream {

    private final RandomAccessFile raf;

    public RAFSeekableOutputStream(RandomAccessFile raf) {
      this.raf = raf;
    }

    @Override
    public void write(long position, byte[] value) throws IOException {
      raf.seek(position);
      raf.write(value);
    }

    @Override
    public FileChannel getChannel() {
      return raf.getChannel();
    }

    @Override
    public void close() throws IOException {
      raf.close();
    }
  }
}
