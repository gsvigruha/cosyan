package com.cosyan.db.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedDataFile {

  private final RandomAccessFile raf;
  private final MappedByteBuffer buffer;
  
  public MappedDataFile(String path) throws FileNotFoundException, IOException {
    raf = new RandomAccessFile(path, "rw");
    buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length());
  }

  public MappedByteBuffer getBuffer() {
    return buffer;
  }

  public void close() throws IOException {
    buffer.force();
    raf.close();
  }
}
