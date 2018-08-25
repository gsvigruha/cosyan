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
package com.cosyan.db.transaction;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.lang.transaction.Result.TransactionResult;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.TableUniqueIndex;

public class TransactionTest extends UnitTestBase {
  @Test
  public void testMultipleInserts() throws InterruptedException {
    execute("create table t1 (a integer, b float, c varchar);");
    execute("insert into t1 values(1, 1.0, 'abc');" +
        "insert into t1 values(2, 1.0, 'abc');" +
        "insert into t1 values(3, 1.0, 'abc');");
    QueryResult result = query("select * from t1;");
    assertHeader(new String[] { "a", "b", "c" }, result);
    assertValues(new Object[][] {
        { 1L, 1.0, "abc" },
        { 2L, 1.0, "abc" },
        { 3L, 1.0, "abc" }
    }, result);
  }

  @Test
  public void testMultipleInsertsWithIndex() throws InterruptedException, ModelException, IOException {
    execute("create table t2 (a integer, b float, c varchar, constraint pk_a primary key (a));");
    execute("insert into t2 values(1, 1.0, 'abc');" +
        "insert into t2 values(2, 1.0, 'abc');" +
        "insert into t2 values(3, 1.0, 'abc');");
    QueryResult result = query("select * from t2;");
    assertHeader(new String[] { "a", "b", "c" }, result);
    assertValues(new Object[][] {
        { 1L, 1.0, "abc" },
        { 2L, 1.0, "abc" },
        { 3L, 1.0, "abc" }
    }, result);

    TableUniqueIndex t2a = metaRepo.collectUniqueIndexes(metaRepo.table("admin", "t2")).get("a");
    // Record length: 1 + 4 + (1 + 8) + (1 + 8) + (1 + 4 + 6) + 4 = 38.
    assertEquals(0L, t2a.get(1L)[0]);
    assertEquals(38L, t2a.get(2L)[0]);
    assertEquals(76L, t2a.get(3L)[0]);
  }

  @Test
  public void testInsertAndSelect() throws InterruptedException, ModelException, IOException {
    execute("create table t3 (a varchar);");
    TransactionResult result = transaction("insert into t3 values('a');" +
        "select * from t3;" +
        "insert into t3 values('b');" +
        "select * from t3;");
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(1));
    assertValues(new Object[][] { { "a" } }, (QueryResult) result.getResults().get(1));
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(3));
    assertValues(new Object[][] {
        { "a" },
        { "b" } }, (QueryResult) result.getResults().get(3));

    QueryResult resultAfterCommit = query("select * from t3;");
    assertHeader(new String[] { "a" }, resultAfterCommit);
    assertValues(new Object[][] {
        { "a" },
        { "b" } }, resultAfterCommit);
  }

  @Test
  public void testInsertAndDelete() throws InterruptedException, ModelException, IOException {
    execute("create table t4 (a varchar);");
    TransactionResult result = transaction("insert into t4 values('a');" +
        "insert into t4 values('b');" +
        "delete from t4 where a = 'a';" +
        "select * from t4;" +
        "insert into t4 values('c');" +
        "delete from t4 where a = 'c';" +
        "select * from t4;");
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(3));
    assertValues(new Object[][] { { "b" } }, (QueryResult) result.getResults().get(3));
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(6));
    assertValues(new Object[][] { { "b" } }, (QueryResult) result.getResults().get(6));

    QueryResult resultAfterCommit = query("select * from t4;");
    assertHeader(new String[] { "a" }, resultAfterCommit);
    assertValues(new Object[][] { { "b" } }, resultAfterCommit);
  }

  @Test
  public void testInsertAndUpdate() throws InterruptedException, ModelException, IOException {
    execute("create table t5 (a varchar);");
    TransactionResult result = transaction("insert into t5 values('a');" +
        "insert into t5 values('b');" +
        "update t5 set a = 'c' where a = 'a';" +
        "select * from t5;" +
        "insert into t5 values('d');" +
        "update t5 set a = 'e' where a = 'c';" +
        "select * from t5;");
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(3));
    assertValues(new Object[][] {
        { "b" },
        { "c" } }, (QueryResult) result.getResults().get(3));
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(6));
    assertValues(new Object[][] {
        { "b" },
        { "d" },
        { "e" } }, (QueryResult) result.getResults().get(6));

    QueryResult resultAfterCommit = query("select * from t5;");
    assertHeader(new String[] { "a" }, resultAfterCommit);
    assertValues(new Object[][] {
        { "b" },
        { "d" },
        { "e" } }, resultAfterCommit);
  }

  @Test
  public void testSelectWithIndex() throws InterruptedException, ModelException, IOException {
    execute("create table t6 (a varchar unique);");
    execute("insert into t6 values('a');");
    TransactionResult result = transaction("insert into t6 values('b');" +
        "insert into t6 values('c');" +
        "select * from t6 where a = 'a';" +
        "select * from t6 where a = 'b';" +
        "insert into t6 values('d');" +
        "select * from t6 where a = 'd';");
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(2));
    assertValues(new Object[][] { { "a" } }, (QueryResult) result.getResults().get(2));
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(3));
    assertValues(new Object[][] { { "b" } }, (QueryResult) result.getResults().get(3));
    assertHeader(new String[] { "a" }, (QueryResult) result.getResults().get(5));
    assertValues(new Object[][] { { "d" } }, (QueryResult) result.getResults().get(5));

    QueryResult resultAfterCommit = query("select * from t6 where a = 'b';");
    assertHeader(new String[] { "a" }, resultAfterCommit);
    assertValues(new Object[][] { { "b" } }, resultAfterCommit);
  }

  @Test
  public void testInsertIntoReferencedTableAndSelect() throws InterruptedException, ModelException, IOException {
    execute("create table t7 (a varchar unique, b integer, constraint pk_a primary key (a));");
    execute("create table t8 (a varchar, constraint fk_a foreign key (a) references t7(a));");
    TransactionResult result = transaction("insert into t7 values('x', 1);" +
        "insert into t8 values('x');" +
        "select a, fk_a.b from t8;");
    assertHeader(new String[] { "a", "b" }, (QueryResult) result.getResults().get(2));
    assertValues(new Object[][] { { "x", 1L } }, (QueryResult) result.getResults().get(2));
  }
}
