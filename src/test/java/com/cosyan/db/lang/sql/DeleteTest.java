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

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.TableMultiIndex;
import com.cosyan.db.model.TableUniqueIndex;

public class DeleteTest extends UnitTestBase {

  @Test
  public void testDeleteFromTable() throws Exception {
    execute("create table t1 (a varchar, b integer, c float);");
    execute("insert into t1 values ('x', 1, 2.0);");
    execute("insert into t1 values ('y', 3, 4.0);");
    QueryResult r1 = query("select * from t1;");
    assertHeader(new String[] { "a", "b", "c" }, r1);
    assertValues(new Object[][] {
        { "x", 1L, 2.0 },
        { "y", 3L, 4.0 }
    }, r1);

    execute("delete from t1 where a = 'x';");
    QueryResult r2 = query("select * from t1;");
    assertValues(new Object[][] {
        { "y", 3L, 4.0 }
    }, r2);
  }

  @Test
  public void testDeleteWithIndex() throws Exception {
    execute("create table t2 (a varchar unique not null, b integer);");
    execute("insert into t2 values ('x', 1);");
    execute("insert into t2 values ('y', 2);");
    ErrorResult r1 = error("insert into t2 values ('x', 3);");
    assertError(RuleException.class, "Key 'x' already present in index.", r1);

    execute("delete from t2 where a = 'x';");
    execute("insert into t2 values ('x', 3);");

    QueryResult r2 = query("select * from t2;");
    assertHeader(new String[] { "a", "b" }, r2);
    assertValues(new Object[][] {
        { "y", 2L },
        { "x", 3L }
    }, r2);
  }

  @Test
  public void testDeleteWithForeignKey() throws Exception {
    execute("create table t3 (a varchar, constraint pk_a primary key (a));");
    execute("create table t4 (a varchar, b varchar, constraint fk_b foreign key (b) references t3(a));");
    execute("insert into t3 values ('x');");
    execute("insert into t3 values ('y');");
    execute("insert into t4 values ('123', 'x');");

    ErrorResult r1 = error("delete from t3 where a = 'x';");
    assertError(RuleException.class, "Foreign key violation, key value 'x' has references.", r1);

    execute("delete from t4 where b = 'x';");
    execute("delete from t3 where a = 'x';");

    QueryResult r2 = query("select * from t3;");
    assertHeader(new String[] { "a" }, r2);
    assertValues(new Object[][] { { "y" } }, r2);
  }

  @Test
  public void testDeleteWithForeignKeyIndexes() throws Exception {
    execute("create table t5 (a varchar, constraint pk_a primary key (a));");
    execute("create table t6 (a varchar, b varchar, constraint fk_b foreign key (b) references t5(a));");
    execute("insert into t5 values ('x');");
    execute("insert into t5 values ('y');");
    execute("insert into t6 values ('123', 'x');");

    TableUniqueIndex t5a = metaRepo.collectUniqueIndexes(metaRepo.table("admin", "t5")).get("a");
    assertEquals(0L, t5a.get("x")[0]);
    assertEquals(16L, t5a.get("y")[0]);
    TableMultiIndex t6b = metaRepo.collectMultiIndexes(metaRepo.table("admin", "t6")).get("b");
    org.junit.Assert.assertArrayEquals(new long[] { 0L }, t6b.get("x"));

    execute("delete from t6 where b = 'x';");
    execute("delete from t5 where a = 'x';");

    assertEquals(false, t5a.contains("x"));
    assertEquals(16L, t5a.get("y")[0]);
    org.junit.Assert.assertArrayEquals(new long[0], t6b.get("x"));
  }

  @Test
  public void testMultipleDeletes() throws Exception {
    execute("create table t7 (a varchar, b integer, c float);");
    execute("insert into t7 values ('x', 1, 2.0);");
    execute("insert into t7 values ('y', 3, 4.0);");
    execute("insert into t7 values ('z', 5, 6.0);");
    execute("insert into t7 values ('w', 7, 8.0);");
    QueryResult r1 = query("select * from t7;");
    assertHeader(new String[] { "a", "b", "c" }, r1);
    assertValues(new Object[][] {
        { "x", 1L, 2.0 },
        { "y", 3L, 4.0 },
        { "z", 5L, 6.0 },
        { "w", 7L, 8.0 }
    }, r1);

    execute("delete from t7 where a = 'z';");
    execute("delete from t7 where b = 3;");
    execute("delete from t7 where a = 'x';");
    QueryResult r2 = query("select * from t7;");
    assertValues(new Object[][] {
        { "w", 7L, 8.0 }
    }, r2);
  }

  @Test
  public void testDeleteReferencedByRules_MultiTable() throws Exception {
    execute("create table t8 (a varchar, constraint pk_a primary key (a));");
    execute("create table t9 (b varchar, c integer, constraint fk_a foreign key (b) references t8(a));");
    execute("alter table t8 add aggref s (select sum(c) as sc from rev_fk_a);");
    execute("alter table t8 add constraint c_c check (s.sc > 1);");

    execute("insert into t8 values ('x');");
    execute("insert into t9 values ('x', 3);");
    execute("insert into t9 values ('x', 2);");
    execute("insert into t9 values ('x', 1);");

    execute("delete from t9 where c = 3;");
    QueryResult r1 = query("select b, c, fk_a.a from t9;");
    assertValues(new Object[][] { { "x", 2L, "x" }, { "x", 1L, "x" } }, r1);

    ErrorResult e1 = error("delete from t9 where c = 2;");
    assertEquals("Referencing constraint check t8.c_c failed.", e1.getError().getMessage());
    QueryResult r2 = query("select b, c, fk_a.a from t9;");
    assertValues(new Object[][] { { "x", 2L, "x" }, { "x", 1L, "x" } }, r2);
  }

  @Test
  public void testDeleteReferencedByRules_MultiTableMultipleLevels() throws Exception {
    execute("create table t10 (a varchar, constraint pk_a primary key (a));");
    execute("create table t11 (b varchar, c varchar, "
        + "constraint pk_b primary key (b), "
        + "constraint fk_c foreign key (c) references t10(a));");
    execute("create table t12 (d varchar, e integer, constraint fk_d foreign key (d) references t11(b));");
    execute("alter table t11 add aggref s (select sum(e) as se from rev_fk_d);");
    execute("alter table t10 add aggref s (select sum(s.se) as sse from rev_fk_c);");
    execute("alter table t10 add constraint c_c check (s.sse > 0);");

    execute("insert into t10 values ('x');");
    execute("insert into t11 values ('a', 'x');");
    execute("insert into t12 values ('a', 2), ('a', 3), ('a', -1);");

    execute("delete from t12 where e = 3;");
    QueryResult r1 = query("select e, fk_d.fk_c.a from t12;");
    assertValues(new Object[][] { { 2L, "x" }, { -1L, "x" } }, r1);

    ErrorResult e1 = error("delete from t12 where e = 2;");
    assertEquals("Referencing constraint check t10.c_c failed.", e1.getError().getMessage());
    QueryResult r2 = query("select e, fk_d.fk_c.a from t12;");
    assertValues(new Object[][] { { 2L, "x" }, { -1L, "x" } }, r2);
  }
}
