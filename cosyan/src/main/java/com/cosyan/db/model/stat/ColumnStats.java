package com.cosyan.db.model.stat;

import java.util.zip.CRC32;

import net.agkn.hll.HLL;

public class ColumnStats {

  public static final int CNTS_SIZE = 256;
  private long notNull;
  private HLL hll;
  private long[] cnts;

  public ColumnStats() {
    this.cnts = new long[CNTS_SIZE];
    hll = new HLL(13, 5);
  }

  public void add(Object obj) {
    if (obj != null) {
      notNull++;

      CRC32 crc = new CRC32();
      crc.update(obj.toString().getBytes());
      long hash32 = crc.getValue();

      int address = ((int) hash32) & 0xFF;
      cnts[address]++;
      // TODO: use 64 bit hashing for HLL.
      hll.addRaw(hash32);
    }
  }

  public double maxRelativeCardinality() {
    long max = 0L;
    for (int i = 0; i < CNTS_SIZE; i++) {
      max = Math.max(max, cnts[i]);
    }
    return ((double) max) / notNull;
  }

  public long cardinality() {
    return hll.cardinality();
  }

  public long notNull() {
    return notNull;
  }
}
