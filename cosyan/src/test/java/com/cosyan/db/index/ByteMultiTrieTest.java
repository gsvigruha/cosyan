package com.cosyan.db.index;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;

import org.junit.Test;

import com.cosyan.db.index.ByteMultiTrie.LongMultiIndex;
import com.cosyan.db.index.ByteMultiTrie.StringMultiIndex;

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
}
