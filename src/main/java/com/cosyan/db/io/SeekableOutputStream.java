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
import java.nio.channels.FileChannel;

public interface SeekableOutputStream {

  public void write(long position, byte[] value) throws IOException;

  public FileChannel getChannel();

  public void close() throws IOException;

  public static class RAFSeekableOutputStream implements SeekableOutputStream {

    private final RandomAccessFile raf;

    public RAFSeekableOutputStream(RandomAccessFile raf) {
      this.raf = raf;
    }

    @Override
    public void write(long position, byte[] value) throws IOException {
      raf.seek(position);
      raf.write(value);
    }

    @Override
    public FileChannel getChannel() {
      return raf.getChannel();
    }

    @Override
    public void close() throws IOException {
      raf.close();
    }
  }
}
