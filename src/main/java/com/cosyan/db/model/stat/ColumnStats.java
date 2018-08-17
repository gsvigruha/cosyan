/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.model.stat;

import java.util.zip.CRC32;

import net.agkn.hll.HLL;

public class ColumnStats {

  public static final int CNTS_SIZE = 256;
  private long notNull;
  private final HLL hll;
  private final long[] cnts;

  public ColumnStats() {
    cnts = new long[CNTS_SIZE];
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
