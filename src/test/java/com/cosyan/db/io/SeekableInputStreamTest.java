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
package com.cosyan.db.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.cosyan.db.io.SeekableInputStream.SeekableByteArrayInputStream;
import com.cosyan.db.io.SeekableInputStream.SeekableSequenceInputStream;
import com.google.common.collect.ImmutableList;

public class SeekableInputStreamTest {

  @Test
  public void testReadSeekableByteInputStream() throws IOException {
    SeekableInputStream stream = new SeekableByteArrayInputStream(new byte[] { 1, 127, -1, -128 });
    assertEquals(1, stream.read());
    assertEquals(127, stream.read());
    assertEquals(255, stream.read());
    assertEquals(128, stream.read());
    assertEquals(-1, stream.read());
    stream.close();
  }

  @Test
  public void testSeekSeekableByteInputStream() throws IOException {
    SeekableInputStream stream = new SeekableByteArrayInputStream(new byte[] { 1, 127, -1, -128 });
    stream.seek(3);
    assertEquals(128, stream.read());
    stream.seek(1);
    assertEquals(127, stream.read());
    stream.seek(2);
    assertEquals(255, stream.read());
    stream.seek(0);
    assertEquals(1, stream.read());
    stream.close();
  }

  @Test
  public void testReadSeekableSequenceInputStream() throws IOException {
    SeekableInputStream stream = new SeekableSequenceInputStream(
        new SeekableByteArrayInputStream(new byte[] { 1, 127 }),
        new SeekableByteArrayInputStream(new byte[] { -1, -128 }));
    assertEquals(1, stream.read());
    assertEquals(127, stream.read());
    assertEquals(255, stream.read());
    assertEquals(128, stream.read());
    assertEquals(-1, stream.read());
    stream.close();
  }

  @Test
  public void testSeekSeekableSequenceInputStream() throws IOException {
    SeekableInputStream stream = new SeekableSequenceInputStream(
        new SeekableByteArrayInputStream(new byte[] { 1, 127 }),
        new SeekableByteArrayInputStream(new byte[] { -1, -128 }));
    stream.seek(3);
    assertEquals(128, stream.read());
    stream.seek(1);
    assertEquals(127, stream.read());
    stream.seek(2);
    assertEquals(255, stream.read());
    stream.seek(0);
    assertEquals(1, stream.read());
    stream.close();
  }

  @Test
  public void testReadSeekableSequenceInputStreamEmptyStream() throws IOException {
    SeekableInputStream stream = new SeekableSequenceInputStream(ImmutableList.of(
        new SeekableByteArrayInputStream(new byte[] { 1, 127 }),
        new SeekableByteArrayInputStream(new byte[] {}),
        new SeekableByteArrayInputStream(new byte[] { -1, -128 })));
    assertEquals(1, stream.read());
    assertEquals(127, stream.read());
    assertEquals(255, stream.read());
    assertEquals(128, stream.read());
    assertEquals(-1, stream.read());
    stream.close();
  }

  @Test
  public void testSeekSeekableSequenceInputStreamEmptyStream() throws IOException {
    SeekableInputStream stream = new SeekableSequenceInputStream(ImmutableList.of(
        new SeekableByteArrayInputStream(new byte[] { 1, 127 }),
        new SeekableByteArrayInputStream(new byte[] {}),
        new SeekableByteArrayInputStream(new byte[] { -1, -128 })));
    stream.seek(3);
    assertEquals(128, stream.read());
    stream.seek(1);
    assertEquals(127, stream.read());
    stream.seek(2);
    assertEquals(255, stream.read());
    stream.seek(0);
    assertEquals(1, stream.read());
    stream.close();
  }
}
