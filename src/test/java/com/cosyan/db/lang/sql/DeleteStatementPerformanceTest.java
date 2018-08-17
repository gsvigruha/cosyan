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

import java.util.Random;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.StatementResult;

public class DeleteStatementPerformanceTest extends UnitTestBase {

  private static final int N = 5000;

  @Test
  public void testDeleteWithWhereIndexed() {
    long t = System.currentTimeMillis();
    execute("create table t1 (a varchar unique, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t1 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index inserted in " + t + " " + speed(t, N));
    t = System.currentTimeMillis();
    Random random = new Random();
    int deleted = 0;
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N);
      StatementResult result = stmt("delete from t1 where a = 'abc" + r + "';");
      deleted += result.getAffectedLines();
    }
    assertTrue(deleted > N / 2);
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index deleted in " + t + " " + speed(t, N));
  }

  @Test
  public void testDeleteWithWhereNotIndexed() {
    long t = System.currentTimeMillis();
    execute("create table t2 (a varchar, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t2 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index inserted in " + t + " " + speed(t, N));
    t = System.currentTimeMillis();
    Random random = new Random();
    int deleted = 0;
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N);
      StatementResult result = stmt("delete from t2 where a = 'abc" + r + "';");
      deleted += result.getAffectedLines();
    }
    assertTrue(deleted > N / 2);
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index deleted in " + t + " " + speed(t, N));
  }
}
