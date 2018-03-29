package com.cosyan.db.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.TreeMap;

import com.cosyan.db.index.ByteTrie.IndexException;

public class IDIndex {

  private static final int SIZE = 4096;

  private final TreeMap<Long, long[]> cachedIndices = new TreeMap<>();

  private final String fileName;

  private RandomAccessFile raf;
  private long filePointer;

  public IDIndex(String fileName) throws IOException {
    this.fileName = fileName;
    this.raf = new RandomAccessFile(fileName, "rw");
    filePointer = raf.length();
  }

  public void close() throws IOException {
    cleanUp();
    raf.close();
  }

  public void cleanUp() {
    cachedIndices.clear();
  }

  public void drop() throws IOException {
    close();
    new File(fileName).delete();
  }

  public void reOpen() throws FileNotFoundException {
    this.raf = new RandomAccessFile(fileName, "rw");
  }

  public Long get(long key) throws IOException {
    long segment = key / SIZE;
    long[] cachedValues = cachedIndices.get(segment);
    if (cachedValues == null) {
      if (key >= filePointer) {
        return null;
      }
      cachedValues = read(segment);
    }
    long cachedValue = cachedValues[(int) (key % SIZE)];
    return cachedValue == -1 ? null : cachedValue;
  }

  private long[] read(long segment) throws IOException {
    ByteBuffer bb = ByteBuffer.allocate(SIZE * 8);
    raf.seek(segment * SIZE * 8);
    raf.read(bb.array());
    LongBuffer lb = bb.asLongBuffer();
    long[] cachedValues = new long[lb.capacity()];
    lb.get(cachedValues);
    return cachedValues;
  }

  public void put(long key, long value) throws IOException, IndexException {
    long segment = key / SIZE;
    long[] cachedValues = cachedIndices.get(segment);
    if (cachedValues == null) {
      if (key >= filePointer) {
        cachedValues = new long[SIZE];
        Arrays.fill(cachedValues, -1);
      } else {
        cachedValues = read(segment);
      }
      cachedIndices.put(segment, cachedValues);
    }
    if (cachedValues[(int) (key % SIZE)] != -1) {
      throw new IndexException("Key '" + key + "' already present in index.");
    }
    cachedValues[(int) (key % SIZE)] = value;
  }

  public boolean delete(long key) throws IOException {
    long segment = key / SIZE;
    long[] cachedValues = cachedIndices.get(segment);
    long blockStart = segment * SIZE;
    if (cachedValues == null) {
      if (key >= filePointer) {
        return false;
      }
      cachedValues = read(segment);
      cachedIndices.put(segment, cachedValues);
    }
    int idx = (int) (key - blockStart);
    long cachedValue = cachedValues[idx];
    if (cachedValue == -1) {
      return false;
    } else {
      cachedValues[idx] = -1;
      return true;
    }
  }

  public void commit() throws IOException {
    for (long i = 0; i <= cachedIndices.lastKey(); i++) {
      long[] values = cachedIndices.get(i);
      if (values == null) {
        values = new long[SIZE];
        Arrays.fill(values, -1);
      }
      raf.seek(i * SIZE * 8);
      ByteBuffer lb = ByteBuffer.allocate(SIZE * 8);
      lb.asLongBuffer().put(values);
      raf.write(lb.array());
    }
    filePointer = raf.length();
  }

  public void rollback() {
    cachedIndices.clear();
  }
}
