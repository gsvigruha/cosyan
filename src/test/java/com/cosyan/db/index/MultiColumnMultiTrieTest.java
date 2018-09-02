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
package com.cosyan.db.index;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;

import org.junit.Test;

import com.cosyan.db.index.MultiLeafTries.MultiColumnMultiIndex;
import com.cosyan.db.model.DataTypes;
import com.google.common.collect.ImmutableList;

public class MultiColumnMultiTrieTest {

  private void assertEquals(long[] expected, long[] actual) {
    org.junit.Assert.assertArrayEquals(expected, actual);
  }

  private void assertEquals(boolean expected, boolean actual) {
    org.junit.Assert.assertEquals(expected, actual);
  }

  private void assertEquals(int expected, int actual) {
    org.junit.Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLongStringByteTrie() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/longstringindex#chain"));
    Files.deleteIfExists(Paths.get("/tmp/longstringindex#index"));
    MultiColumnMultiIndex index = new MultiColumnMultiIndex("/tmp/longstringindex",
        ImmutableList.of(DataTypes.LongType, DataTypes.StringType));
    assertEquals(new long[0], index.get(new Object[] { 10L, "a" }));
    index.put(new Object[] { 10L, "a" }, 100L);
    assertEquals(new long[] { 100L }, index.get(new Object[] { 10L, "a" }));
    index.commit();
    assertEquals(new long[] { 100L }, index.get(new Object[] { 10L, "a" }));
    LinkedList<Long> v10L = new LinkedList<>();
    v10L.add(100L);
    for (int i = 2; i < 100; i++) {
      long value = i * 100;
      index.put(new Object[] { 10L, "a" }, value);
      v10L.add(value);
    }
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(new Object[] { 10L, "a" }));
    index.commit();
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(new Object[] { 10L, "a" }));
    index.put(new Object[] { 20L, "bbb" }, 200L);
    assertEquals(new long[] { 200L }, index.get(new Object[] { 20L, "bbb" }));
    index.rollback();
    assertEquals(new long[0], index.get(new Object[] { 20L, "bbb" }));

    assertEquals(true, index.delete(new Object[] { 10L, "a" }, 500L));
    v10L.remove(500L);
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(new Object[] { 10L, "a" }));
    index.commit();
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(new Object[] { 10L, "a" }));
    assertEquals(false, index.delete(new Object[] { 10L, "a" }, 500L));

    index.delete(new Object[] { 10L, "a" }, 1000L);
    v10L.remove(1000L);
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(new Object[] { 10L, "a" }));
    index.rollback();
    assertEquals(98, index.get(new Object[] { 10L, "a" }).length);

    index.delete(new Object[] { 10L, "a" });
    assertEquals(new long[0], index.get(new Object[] { 10L, "a" }));
    index.commit();

    assertEquals(false, index.delete(new Object[] { 30L, "cc" }));
  }

  @Test
  public void testStringLongByteTrieFrequentCommit() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/stringlongindex2#chain"));
    Files.deleteIfExists(Paths.get("/tmp/stringlongindex2#index"));
    MultiColumnMultiIndex index = new MultiColumnMultiIndex("/tmp/stringlongindex2",
        ImmutableList.of(DataTypes.LongType, DataTypes.StringType));
    LinkedList<Long> v10L = new LinkedList<>();
    for (int i = 2; i < 100; i++) {
      long value = i * 100;
      index.put(new Object[] { 10L, "a" }, value);
      index.commit();
      v10L.add(value);
    }
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(new Object[] { 10L, "a" }));
  }
}
