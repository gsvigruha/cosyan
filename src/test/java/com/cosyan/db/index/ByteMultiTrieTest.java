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

import com.cosyan.db.index.MultiLeafTries.DoubleMultiIndex;
import com.cosyan.db.index.MultiLeafTries.LongMultiIndex;
import com.cosyan.db.index.MultiLeafTries.StringMultiIndex;

public class ByteMultiTrieTest {

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
  public void testLongByteTrie() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/longindex#chain"));
    Files.deleteIfExists(Paths.get("/tmp/longindex#index"));
    LongMultiIndex index = new LongMultiIndex("/tmp/longindex");
    assertEquals(new long[0], index.get(10L));
    index.put(10L, 100L);
    assertEquals(new long[] { 100L }, index.get(10L));
    index.commit();
    assertEquals(new long[] { 100L }, index.get(10L));
    LinkedList<Long> v10L = new LinkedList<>();
    v10L.add(100L);
    for (int i = 2; i < 100; i++) {
      long value = i * 100;
      index.put(10L, value);
      v10L.add(value);
    }
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10L));
    index.commit();
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10L));
    index.put(20L, 200L);
    assertEquals(new long[] { 200L }, index.get(20L));
    index.rollback();
    assertEquals(new long[0], index.get(20L));

    assertEquals(true, index.delete(10L, 500L));
    v10L.remove(500L);
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10L));
    index.commit();
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10L));
    assertEquals(false, index.delete(10L, 500L));

    index.delete(10L, 1000L);
    v10L.remove(1000L);
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10L));
    index.rollback();
    assertEquals(98, index.get(10L).length);

    index.delete(10L);
    assertEquals(new long[0], index.get(10L));
    index.commit();

    assertEquals(false, index.delete(30L));
  }

  @Test
  public void testDoubleByteTrie() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/doubleindex#chain"));
    Files.deleteIfExists(Paths.get("/tmp/doubleindex#index"));
    DoubleMultiIndex index = new DoubleMultiIndex("/tmp/doubleindex");
    assertEquals(new long[0], index.get(10.0));
    index.put(10.0, 100L);
    assertEquals(new long[] { 100L }, index.get(10.0));
    index.commit();
    assertEquals(new long[] { 100L }, index.get(10.0));
    LinkedList<Long> v10L = new LinkedList<>();
    v10L.add(100L);
    for (int i = 2; i < 100; i++) {
      long value = i * 100;
      index.put(10.0, value);
      v10L.add(value);
    }
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10.0));
    index.commit();
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10.0));
    index.put(20.0, 200L);
    assertEquals(new long[] { 200L }, index.get(20.0));
    index.rollback();
    assertEquals(new long[0], index.get(20.0));

    assertEquals(true, index.delete(10.0, 500L));
    v10L.remove(500L);
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10.0));
    index.commit();
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10.0));
    assertEquals(false, index.delete(10.0, 500L));

    index.delete(10.0, 1000L);
    v10L.remove(1000L);
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10.0));
    index.rollback();
    assertEquals(98, index.get(10.0).length);

    index.delete(10.0);
    assertEquals(new long[0], index.get(10.0));
    index.commit();

    assertEquals(false, index.delete(30.0));
  }

  @Test
  public void testStringByteTrie() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/stringindex#chain"));
    Files.deleteIfExists(Paths.get("/tmp/stringindex#index"));
    StringMultiIndex index = new StringMultiIndex("/tmp/stringindex");
    assertEquals(new long[0], index.get("a"));
    index.put("a", 100L);
    assertEquals(new long[] { 100L }, index.get("a"));
    index.commit();
    assertEquals(new long[] { 100L }, index.get("a"));
    LinkedList<Long> v10L = new LinkedList<>();
    v10L.add(100L);
    for (int i = 2; i < 100; i++) {
      long value = i * 100;
      index.put("a", value);
      v10L.add(value);
    }
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get("a"));
    index.commit();
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get("a"));
    index.put("aa", 200L);
    assertEquals(new long[] { 200L }, index.get("aa"));
    index.rollback();
    assertEquals(new long[0], index.get("aa"));

    assertEquals(true, index.delete("a", 500L));
    v10L.remove(500L);
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get("a"));
    index.commit();
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get("a"));
    assertEquals(false, index.delete("a", 500L));

    index.delete("a", 1000L);
    v10L.remove(1000L);
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get("a"));
    index.rollback();
    assertEquals(98, index.get("a").length);

    index.delete("a");
    assertEquals(new long[0], index.get("a"));
    index.commit();

    assertEquals(false, index.delete("x"));
  }

  @Test
  public void testLongByteTrieFrequentCommit() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/longindex2#chain"));
    Files.deleteIfExists(Paths.get("/tmp/longindex2#index"));
    LongMultiIndex index = new LongMultiIndex("/tmp/longindex2");
    LinkedList<Long> v10L = new LinkedList<>();
    for (int i = 2; i < 100; i++) {
      long value = i * 100;
      index.put(10L, value);
      index.commit();
      v10L.add(value);
    }
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10L));
  }

  @Test
  public void testStringByteTrieFrequentCommit() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/stringindex2#chain"));
    Files.deleteIfExists(Paths.get("/tmp/stringindex2#index"));
    StringMultiIndex index = new StringMultiIndex("/tmp/stringindex2");
    LinkedList<Long> v10L = new LinkedList<>();
    for (int i = 2; i < 100; i++) {
      long value = i * 100;
      index.put("a", value);
      index.commit();
      v10L.add(value);
    }
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get("a"));
  }

  @Test
  public void testDoubleByteTrieFrequentCommit() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/doubleindex2#chain"));
    Files.deleteIfExists(Paths.get("/tmp/doubleindex2#index"));
    DoubleMultiIndex index = new DoubleMultiIndex("/tmp/doubleindex2");
    LinkedList<Long> v10L = new LinkedList<>();
    for (int i = 2; i < 100; i++) {
      long value = i * 100;
      index.put(10.0, value);
      index.commit();
      v10L.add(value);
    }
    assertEquals(v10L.stream().mapToLong(Long::longValue).toArray(), index.get(10.0));
  }
}
