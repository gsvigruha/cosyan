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
package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.session.Session;

public class InsertStatementMultithreadPerformanceTest extends UnitTestBase {

  private static final int N = 20000;
  private static final int T = 4;

  private Thread runXTimes(String sql, int x, int offset) {
    return new Thread() {
      public void run() {
        Session s = dbApi.newAdminSession();
        for (int i = 0; i < x; i++) {
          int j = i + offset;
          Result r = s.execute(sql.replace("$x", String.valueOf(j)));
          assertTrue(r.toJSON().toString(), r.isSuccess());
        }
      }
    };
  }

  @Test
  public void testParallelInsertIntoOneTable() throws InterruptedException {
    long t = System.currentTimeMillis();
    execute("create table t1 (a varchar unique, b integer);");
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < T; i++) {
      threads.add(runXTimes("insert into t1 values ('abc$x', $x);", N / T, (N / T) * i));
    }
    threads.forEach(thread -> thread.start());
    threads.forEach(thread -> {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });

    t = System.currentTimeMillis() - t;
    System.out.println("Records with index one per transaction inserted in " + t + " " + speed(t, N));
  }
}
