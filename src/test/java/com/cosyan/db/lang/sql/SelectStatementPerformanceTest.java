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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.QueryResult;

public class SelectStatementPerformanceTest extends UnitTestBase {

  private static final int N = 5000;

  @Test
  public void testSelectWithWhereIndexed() {
    long t = System.currentTimeMillis();
    execute("create table t1 (a varchar unique, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t1 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index inserted in " + t + " " + speed(t, N));
    t = System.currentTimeMillis();
    Random random = new Random();
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N);
      QueryResult result = query("select * from t1 where a = 'abc" + r + "';");
      assertValues(new Object[][] { { "abc" + r, (long) r } }, result);
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index queried in " + t + " " + speed(t, N));
  }

  @Test
  public void testSelectWithWhereNotIndexed() {
    long t = System.currentTimeMillis();
    execute("create table t2 (a varchar, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t2 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index inserted in " + t + " " + speed(t, N));
    t = System.currentTimeMillis();
    Random random = new Random();
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N);
      QueryResult result = query("select * from t2 where a = 'abc" + r + "';");
      assertValues(new Object[][] { { "abc" + r, (long) r } }, result);
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index queried in " + t + " " + speed(t, N));
  }

  @Test
  public void testSelectWithLookups() {
    final int N = 100000;
    final int K = 10;
    execute("create table t3 (a varchar, b integer, c float, d integer, e float, constraint pk_a primary key (a));");
    execute("create table t4 (a varchar, constraint fk_a foreign key (a) references t3(a));");
    for (int i = 0; i < K; i++) {
      execute("insert into t3 values ('abc" + i + "', " + (i % 2) + " ," + (i % 3) + ".0 ," + (i % 5) + " ," + (i % 7) + ".0);");
    }
    for (int i = 0; i < N; i++) {
      execute("insert into t4 values ('abc" + (i % K) + "');");
    }
    long t = System.currentTimeMillis();
    t = System.currentTimeMillis();
    QueryResult result = query("select sum(fk_a.b * fk_a.c + fk_a.d * fk_a.e) from t4;");
    assertEquals(580000.0, result.getValues().get(0)[0]);
    t = System.currentTimeMillis() - t;
    System.out.println("Records aggregated in " + t + " " + speed(t, N));
  }
}
