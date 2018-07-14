package com.cosyan.db.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.collect.ImmutableList;

public abstract class SeekableInputStream extends InputStream {

  public abstract void seek(long position) throws IOException;

  public abstract long length();

  public static class SeekableByteArrayInputStream extends SeekableInputStream {

    private final ByteArrayInputStream stream;
    private final long length;

    public SeekableByteArrayInputStream(byte[] array) {
      this.stream = new ByteArrayInputStream(array);
      this.length = array.length;
    }

    @Override
    public void seek(long position) throws IOException {
      stream.reset();
      stream.skip(position);
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public int read() throws IOException {
      return stream.read();
    }
  }

  public static class SeekableSequenceInputStream extends SeekableInputStream {

    private final ImmutableList<SeekableInputStream> streams;
    private long length;

    private SeekableInputStream actStream;
    private int actStreamPointer;

    public SeekableSequenceInputStream(Iterable<SeekableInputStream> streams) {
      this.streams = ImmutableList.copyOf(streams);
      this.length = this.streams.stream().mapToLong(stream -> stream.length()).sum();
      this.actStreamPointer = 0;
      this.actStream = this.streams.get(0);
    }

    public SeekableSequenceInputStream(SeekableInputStream first, SeekableInputStream second) {
      this(ImmutableList.of(first, second));
    }

    @Override
    public void seek(long position) throws IOException {
      int i = 0;
      actStream = streams.get(0);
      while (position >= actStream.length()) {
        if (i == streams.size() - 1) {
          throw new IOException("Position " + position + " out of range.");
        }
        position -= actStream.length();
        actStream = streams.get(++i);
      }
      actStream.seek(position);
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public int read() throws IOException {
      int value = actStream.read();
      while (value == -1) {
        if (actStreamPointer < streams.size() - 1) {
          actStreamPointer++;
          actStream = streams.get(actStreamPointer);
          value = actStream.read();
        } else {
          return -1;
        }
      }
      return value;
    }
  }
}
