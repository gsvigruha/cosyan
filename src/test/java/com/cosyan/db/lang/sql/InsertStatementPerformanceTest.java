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

import org.junit.Test;

import com.cosyan.db.UnitTestBase;

public class InsertStatementPerformanceTest extends UnitTestBase {

  private static final int N = 20000;
  private static final int T = 1000;

  @Test
  public void testInsertWithIndex_oneRecordPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t1 (a varchar unique, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t1 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index one per transaction inserted in " + t + " " + speed(t, N));
  }

  @Test
  public void testInsertWithoutIndex_oneRecordPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t2 (a varchar, b integer);");
    for (int i = 0; i < N; i++) {
      execute("insert into t2 values ('abc" + i + "' ," + i + ");");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index one per transaction inserted in " + t + " " + speed(t, N));
  }

  @Test
  public void testInsertWithIndex_multipleRecordsPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t3 (a varchar unique, b integer);");
    for (int i = 0; i < N / T; i++) {
      StringBuilder sb = new StringBuilder();
      int x = i * T;
      sb.append("insert into t3 values ('abc" + x + "' ," + x + ")");
      for (int j = 1; j < T; j++) {
        x++;
        sb.append(",('abc" + x + "' ," + x + ")");
      }
      sb.append(";");
      execute(sb.toString());
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with index many per transaction inserted in " + t + " " + speed(t, N));
  }

  @Test
  public void testInsertWithoutIndex_multipleRecordsPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t4 (a varchar, b integer);");
    for (int i = 0; i < N / T; i++) {
      StringBuilder sb = new StringBuilder();
      int x = i * T;
      sb.append("insert into t4 values ('abc" + x + "' ," + x + ")");
      for (int j = 1; j < T; j++) {
        x++;
        sb.append(",('abc" + x + "' ," + x + ")");
      }
      sb.append(";");
      execute(sb.toString());
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records without index many per transaction inserted in " + t + " " + speed(t, N));
  }

  @Test
  public void testInsertWithIDIndex_oneRecordPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t5 (a id unique, b varchar);");
    for (int i = 0; i < N; i++) {
      execute("insert into t5 values ('abc" + i + "');");
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with ID index one per transaction inserted in " + t + " " + speed(t, N));
  }

  @Test
  public void testInsertWithIDIndex_multipleRecordsPerTransaction() {
    long t = System.currentTimeMillis();
    execute("create table t6 (a id unique, b varchar);");
    for (int i = 0; i < N / T; i++) {
      StringBuilder sb = new StringBuilder();
      int x = i * T;
      sb.append("insert into t6 values ('abc" + x + "')");
      for (int j = 1; j < T; j++) {
        x++;
        sb.append(",('abc" + x + "')");
      }
      sb.append(";");
      execute(sb.toString());
    }
    t = System.currentTimeMillis() - t;
    System.out.println("Records with ID index many per transaction inserted in " + t + " " + speed(t, N));
  }
}
