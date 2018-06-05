package com.cosyan.db.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.IndexStat.ByteTrieStat;

public class IDIndex {

  private static final int SIZE = 4096;
  private static final int BYTE_SIZE = SIZE * 8;

  private final TreeMap<Long, long[]> cachedIndices = new TreeMap<>();
  private final HashSet<Long> dirty = new HashSet<>();

  private final String fileName;

  private RandomAccessFile raf;
  private long filePointer;
  private long lastID;

  public IDIndex(String fileName) throws IOException {
    this.fileName = fileName;
    this.raf = new RandomAccessFile(fileName, "rw");
    filePointer = raf.length();

    if (filePointer > 0) {
      long lastSegmentID = (filePointer / BYTE_SIZE) - 1;
      long[] lastSegment = read(lastSegmentID);
      for (int i = 0; i < lastSegment.length; i++) {
        if (lastSegment[i] > -1) {
          lastID = lastSegmentID * SIZE + i;
        }
      }
    } else {
      lastID = -1L;
    }
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
      if (key * 8 >= filePointer) {
        return null;
      }
      cachedValues = read(segment);
    }
    long cachedValue = cachedValues[(int) (key % SIZE)];
    return cachedValue == -1 ? null : cachedValue;
  }

  private long[] read(long segment) throws IOException {
    ByteBuffer bb = ByteBuffer.allocate(BYTE_SIZE);
    raf.seek(segment * BYTE_SIZE);
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
      if (key * 8 >= filePointer) {
        for (long i = filePointer / BYTE_SIZE; i <= segment; i++) {
          if (!cachedIndices.containsKey(i)) {
            cachedValues = new long[SIZE];
            Arrays.fill(cachedValues, -1);
            cachedIndices.put(i, cachedValues);
            dirty.add(i);
          }
        }
      } else {
        cachedValues = read(segment);
        cachedIndices.put(segment, cachedValues);
      }
    }
    if (cachedValues[(int) (key % SIZE)] != -1) {
      throw new IndexException("Key '" + key + "' already present in index.");
    }
    dirty.add(segment);
    cachedValues[(int) (key % SIZE)] = value;
    lastID = Math.max(lastID, key);
  }

  public boolean delete(long key) throws IOException {
    long segment = key / SIZE;
    long[] cachedValues = cachedIndices.get(segment);
    long blockStart = segment * SIZE;
    if (cachedValues == null) {
      if (key * 8 >= filePointer) {
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
      dirty.add(segment);
      cachedValues[idx] = -1;
      return true;
    }
  }

  public void commit() throws IOException {
    for (Map.Entry<Long, long[]> e : cachedIndices.entrySet()) {
      long i = e.getKey();
      if (dirty.contains(i)) {
        long[] values = e.getValue();
        raf.seek(i * BYTE_SIZE);
        ByteBuffer lb = ByteBuffer.allocate(BYTE_SIZE);
        lb.asLongBuffer().put(values);
        raf.write(lb.array());
      }
    }
    filePointer = raf.length();
    dirty.clear();
  }

  public void rollback() {
    cachedIndices.clear();
    dirty.clear();
  }

  public long getLastID() {
    return lastID;
  }

  public ByteTrieStat stats() throws IOException {
    return new ByteTrieStat(raf.length(), cachedIndices.size(), dirty.size());
  }
}
