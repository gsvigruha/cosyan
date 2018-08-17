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
package com.cosyan.db.tools;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.logging.MetaJournal.DBException;

public class BackupManagerPerformanceTest extends UnitTestBase {

  private static final int N = 1000000;
  private static final int T = 10000;

  @Test
  public void testBackupSpeed() throws IOException, DBException {
    BackupManager backupManager = new BackupManager(config, metaRepo);
    execute("create table t1 (a varchar unique, b integer, c float);");
    for (int i = 0; i < N / T; i++) {
      StringBuilder sb = new StringBuilder();
      for (int j = 0; j < T; j++) {
        int k = j + i * T;
        sb.append("insert into t1 values ('abc" + k + "' ," + k + ", " + k + ".0);");
      }
      execute(sb.toString());
    }

    long t = System.currentTimeMillis();
    backupManager.backup("my_backup.zip");
    t = System.currentTimeMillis() - t;
    File backupFile = new File("/tmp/data/backup/my_backup.zip");
    long size = backupFile.length();
    System.out.println("Directories backed up in " + t + ", (size: " + size + ").");

    t = System.currentTimeMillis();
    backupManager.restore("my_backup.zip");
    t = System.currentTimeMillis() - t;
    System.out.println("Directories restored in " + t + ", (size: " + size + ").");
  }
}
