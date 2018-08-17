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

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.model.DataTypes.DataType;

public class Indexes {

  public static interface IndexReader {

    public boolean contains(Object key) throws IOException;

    public long[] get(Object key) throws IOException;
    
    public DataType<?> keyDataType();
  }

  public static interface IndexWriter {

    public abstract void put(Object key, long fileIndex) throws IOException, IndexException;

    public abstract boolean delete(Object key) throws IOException;

  }
}
