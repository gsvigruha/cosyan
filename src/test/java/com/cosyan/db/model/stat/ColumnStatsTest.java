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

import static org.junit.Assert.*;

import org.junit.Test;

public class ColumnStatsTest {

  @Test
  public void testCntsUnequal() {
    ColumnStats s1 = new ColumnStats();
    for (int i = 0; i < 10000; i++) {
      s1.add("abc");
      s1.add("xyz" + i);
    }
    assertEquals(0.5, s1.maxRelativeCardinality(), 0.01);
  }

  @Test
  public void testCntsEqual() {
    ColumnStats s1 = new ColumnStats();
    for (int i = 0; i < 10000; i++) {
      s1.add("xyz" + i);
    }
    assertEquals(0.004, s1.maxRelativeCardinality(), 0.001);
  }

  @Test
  public void testCntsLongtail() {
    ColumnStats s1 = new ColumnStats();
    for (int i = 0; i < 10000; i++) {
      s1.add("xyz" + (int) (Math.log(i) / Math.log(2)));
    }
    assertEquals(0.4, s1.maxRelativeCardinality(), 0.01);
  }

  @Test
  public void testHLLUnequal() {
    ColumnStats s1 = new ColumnStats();
    for (int i = 0; i < 10000; i++) {
      s1.add("abc");
      s1.add("xyz" + i);
    }
    assertEquals(10000L, s1.cardinality(), 1000);
  }

  @Test
  public void testHLLEqual() {
    ColumnStats s1 = new ColumnStats();
    for (int i = 0; i < 10000; i++) {
      s1.add("xyz" + i);
    }
    assertEquals(10000L, s1.cardinality(), 1000);
  }

  @Test
  public void testHLLLongtail() {
    ColumnStats s1 = new ColumnStats();
    for (int i = 0; i < 10000; i++) {
      s1.add("xyz" + (int) (Math.log(i) / Math.log(2)));
    }
    assertEquals(15L, s1.cardinality(), 1);
  }
}
