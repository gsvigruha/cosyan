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

import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.index.LongLeafTries.MultiColumnIndex;
import com.cosyan.db.model.DataTypes;
import com.google.common.collect.ImmutableList;

public class MultiKeyByteTrieTest {

  private void assertEquals(Long expected, Long actual) {
    org.junit.Assert.assertEquals(expected, actual);
  }

  @Test
  public void testStringLongByteTrie() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/stringlongindex"));
    MultiColumnIndex index = new MultiColumnIndex("/tmp/stringlongindex", ImmutableList.of(DataTypes.StringType, DataTypes.LongType));
    assertEquals(null, index.get(new Object[] { "a", 1L }));
    index.put(new Object[] { "a", 1L }, 10L);
    assertEquals(10L, index.get(new Object[] { "a", 1L }));
    index.commit();
    assertEquals(10L, index.get(new Object[] { "a", 1L }));

    assertEquals(null, index.get(new Object[] { "b", 1L }));
    assertEquals(null, index.get(new Object[] { "a", 2L }));
    index.put(new Object[] { "b", 1L }, 20L);
    index.put(new Object[] { "a", 2L }, 30L);

    assertEquals(20L, index.get(new Object[] { "b", 1L }));
    assertEquals(30L, index.get(new Object[] { "a", 2L }));
    index.delete(new Object[] { "b", 1L });
    assertEquals(null, index.get(new Object[] { "b", 1L }));
    index.commit();
    assertEquals(null, index.get(new Object[] { "b", 1L }));

    index.delete(new Object[] { "a", 2L });
    assertEquals(null, index.get(new Object[] { "a", 2L }));
    index.commit();
    assertEquals(null, index.get(new Object[] { "a", 2L }));

    index.put(new Object[] { "aaaaaaaaa", 1L }, 40L);
    assertEquals(40L, index.get(new Object[] { "aaaaaaaaa", 1L }));
    index.commit();
    assertEquals(40L, index.get(new Object[] { "aaaaaaaaa", 1L }));
  }

  @Test
  public void testStringLongByteTrieDuplicateKeys() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/stringlongindex"));
    MultiColumnIndex index = new MultiColumnIndex("/tmp/stringlongindex", ImmutableList.of(DataTypes.StringType, DataTypes.LongType));
    assertEquals(null, index.get(new Object[] { "a", 1L }));
    index.put(new Object[] { "a", 1L }, 10L);
    assertEquals(10L, index.get(new Object[] { "a", 1L }));
    try {
      index.put(new Object[] { "a", 1L }, 10L);
      fail();
    } catch (IndexException e) {
    }
  }

  @Test
  public void testLongStringLongByteTrie() throws Exception {
    Files.deleteIfExists(Paths.get("/tmp/longstringlongindex"));
    MultiColumnIndex index = new MultiColumnIndex("/tmp/longstringlongindex",
        ImmutableList.of(DataTypes.LongType, DataTypes.StringType, DataTypes.LongType));
    assertEquals(null, index.get(new Object[] { 1L, "a", 1L }));
    index.put(new Object[] { 1L, "a", 1L }, 10L);
    assertEquals(10L, index.get(new Object[] { 1L, "a", 1L }));
    index.commit();
    assertEquals(10L, index.get(new Object[] { 1L, "a", 1L }));
  }
}
