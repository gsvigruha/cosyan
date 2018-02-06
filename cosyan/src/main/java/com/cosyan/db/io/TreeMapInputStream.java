package com.cosyan.db.io;

import java.io.IOException;
import java.util.TreeMap;

public class TreeMapInputStream extends SeekableInputStream {

  private final TreeMap<Long, byte[]> map;
  private byte[] act;
  private int pointerInAct;
  private long pointer;

  public TreeMapInputStream(TreeMap<Long, byte[]> map) throws IOException {
    this.map = map;
    if (!map.isEmpty()) {
      seek(map.firstKey());
    }
  }

  @Override
  public void seek(long position) throws IOException {
    long offsetPosition = position + map.firstKey();
    act = map.get(offsetPosition);
    if (act == null) {
      throw new IOException("Invalid position " + position + ".");
    }
    pointer = offsetPosition;
    pointerInAct = 0;
  }

  @Override
  public long length() {
    if (map.isEmpty()) {
      return 0;
    }
    return map.lastKey() - map.firstKey() + map.firstEntry().getValue().length;
  }

  @Override
  public int read() throws IOException {
    if (act == null) {
      return -1;
    }
    if (pointerInAct < act.length) {
      return act[pointerInAct++] & 0xff;
    } else {
      Long next = map.ceilingKey(pointer + 1);
      if (next == null) {
        return -1;
      }
      seek(next);
      return read();
    }
  }

}
