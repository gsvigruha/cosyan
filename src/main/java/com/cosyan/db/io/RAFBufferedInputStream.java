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

import java.io.IOException;
import java.io.RandomAccessFile;

public class RAFBufferedInputStream extends SeekableInputStream {

  public static final int DEFAULT_BUFFER_SIZE = 65536;

  private final RandomAccessFile file;

  private final byte[] buffer;

  private long length;

  private int pointer;

  private long totalPointer;

  public RAFBufferedInputStream(RandomAccessFile file) throws IOException {
    this.file = file;
    this.buffer = new byte[DEFAULT_BUFFER_SIZE];
    this.pointer = DEFAULT_BUFFER_SIZE;
    this.totalPointer = 0;
    length = file.length();
  }

  @Override
  public int read() throws IOException {
    if (totalPointer == length) {
      return -1;
    }
    if (pointer == buffer.length) {
      // End of buffer, reload.
      if (totalPointer + buffer.length <= length) {
        // Read the whole buffer.
        file.read(buffer);
      } else {
        // Read the end of the file.
        file.read(buffer, 0, (int) (length - totalPointer));
      }
      pointer = 0;
    }
    totalPointer++;
    return buffer[pointer++] & 0xff;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public void seek(long position) throws IOException {
    long bufferStart = totalPointer - pointer;
    if (position < bufferStart || position >= bufferStart + buffer.length) {
      pointer = buffer.length;
      totalPointer = position;
      file.seek(position);
    } else {
      totalPointer = position;
      pointer = (int) (totalPointer - bufferStart);
    }
  }

  public Object position() {
    return totalPointer;
  }

  public void reset() throws IOException {
    pointer = buffer.length;
    totalPointer = 0;
    length = file.length();
  }
}
