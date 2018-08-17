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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.cosyan.db.conf.Config;

public class TableStats {

  private final Config config;
  private final String tableName;

  private long cnt = 0L;

  public TableStats(Config config, String tableName) throws IOException {
    this.config = config;
    this.tableName = tableName;
  }

  public void load() throws IOException {
    File statFile = new File(config.statDir() + File.separator + tableName);
    if (statFile.exists()) {
      DataInputStream stream = new DataInputStream(new FileInputStream(statFile));
      cnt = stream.readLong();
      stream.close();
    }
  }

  public void save() throws IOException {
    File statFile = new File(config.statDir() + File.separator + tableName);
    if (statFile.exists()) {
      DataOutputStream stream = new DataOutputStream(new FileOutputStream(statFile));
      stream.writeLong(cnt);
      stream.close();
    }
  }

  public boolean isEmpty() {
    return cnt == 0;
  }

  public void insert(long insertedLines) {
    cnt += insertedLines;
  }

  public void delete(long deletedLines) {
    cnt -= deletedLines;
  }
}
