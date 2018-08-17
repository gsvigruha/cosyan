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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedDataFile {

  private final RandomAccessFile raf;
  private final MappedByteBuffer buffer;
  
  public MappedDataFile(String path) throws FileNotFoundException, IOException {
    raf = new RandomAccessFile(path, "rw");
    buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length());
  }

  public MappedByteBuffer getBuffer() {
    return buffer;
  }

  public void close() throws IOException {
    buffer.force();
    raf.close();
  }
}
