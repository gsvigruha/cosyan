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
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.auth.Authenticator.Method;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.Session;

public class SelectStatementTest extends UnitTestBase {

  @Test
  public void testValuesFromDependentTable() {
    execute("create table t1 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t2 (a2 varchar, b2 varchar, constraint fk_a foreign key (a2) references t1(a1));");
    execute("insert into t1 values ('x', 1), ('y', 2);");
    execute("insert into t2 values ('x', 'y'), ('x', 'z');");
    QueryResult r1 = query("select a2, b2, fk_a.a1, fk_a.b1 from t2;");
    assertHeader(new String[] { "a2", "b2", "a1", "b1" }, r1);
    assertValues(new Object[][] {
        { "x", "y", "x", 1L },
        { "x", "z", "x", 1L } }, r1);

    QueryResult r2 = query("select fk_a.a1.length() as a, fk_a.b1 + 1 as b from t2;");
    assertHeader(new String[] { "a", "b" }, r2);
    assertValues(new Object[][] {
        { 1L, 2L },
        { 1L, 2L } }, r2);
  }

  @Test
  public void testValuesFromDependentTableMultipleLevels() {
    execute("create table t3 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t4 (a2 varchar, b2 varchar, constraint pk_a primary key (a2), "
        + "constraint fk_b foreign key (b2) references t3(a1));");
    execute("create table t5 (a3 varchar, constraint fk_a foreign key (a3) references t4(a2));");
    execute("insert into t3 values ('x', 1);");
    execute("insert into t4 values ('a', 'x'), ('b', 'x');");
    execute("insert into t5 values ('a'), ('b');");
    QueryResult result = query("select a3, fk_a.a2, fk_a.b2, fk_a.fk_b.a1, fk_a.fk_b.b1 from t5;");
    assertHeader(new String[] { "a3", "a2", "b2", "a1", "b1" }, result);
    assertValues(new Object[][] {
        { "a", "a", "x", "x", 1L },
        { "b", "b", "x", "x", 1L } }, result);
  }

  @Test
  public void testMultipleForeignKeysToSameTable() {
    execute("create table t6 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t7 (a2 varchar, b2 varchar, "
        + "constraint fk_a foreign key (a2) references t6(a1), "
        + "constraint fk_b foreign key (b2) references t6(a1));");
    execute("insert into t6 values ('x', 1), ('y', 2);");
    execute("insert into t7 values ('x', 'y');");
    QueryResult result = query("select fk_a.a1, fk_a.b1, fk_b.a1 as a2, fk_b.b1 as b2 from t7;");
    assertHeader(new String[] { "a1", "b1", "a2", "b2" }, result);
    assertValues(new Object[][] { { "x", 1L, "y", 2L } }, result);
  }

  @Test
  public void testForeignKeysInAggregation() {
    execute("create table t8 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t9 (a2 varchar, constraint fk_a foreign key (a2) references t8(a1));");
    execute("insert into t8 values ('x', 1), ('y', 2);");
    execute("insert into t9 values ('x'), ('x'), ('x'), ('y');");

    QueryResult r1 = query("select a2, sum(fk_a.b1) as s from t9 group by a2;");
    assertHeader(new String[] { "a2", "s" }, r1);
    assertValues(new Object[][] { { "x", 3L }, { "y", 2L } }, r1);

    QueryResult r2 = query("select a1, sum(fk_a.b1) as s from t9 group by fk_a.a1 as a1;");
    assertHeader(new String[] { "a1", "s" }, r2);
    assertValues(new Object[][] { { "x", 3L }, { "y", 2L } }, r2);

    QueryResult r3 = query("select sum(fk_a.b1) as s from t9;");
    assertHeader(new String[] { "s" }, r3);
    assertValues(new Object[][] { { 5L } }, r3);

    QueryResult r4 = query("select a1 from t9 group by fk_a.a1 as a1;");
    assertHeader(new String[] { "a1" }, r4);
    assertValues(new Object[][] { { "x" }, { "y" } }, r4);
  }

  @Test
  public void testReverseForeignKeys() {
    execute("create table t10 (a1 varchar, constraint pk_a primary key (a1));");
    execute("create table t11 (a2 varchar, b2 integer, constraint fk_a foreign key (a2) references t10(a1));");
    execute("alter table t10 add view s (select sum(b2) as sb, count(b2) as cb from rev_fk_a);");
    execute("insert into t10 values ('x');");
    execute("insert into t11 values ('x', 1), ('x', 5);");

    QueryResult r1 = query("select a1, s.sb from t10;");
    assertHeader(new String[] { "a1", "sb" }, r1);
    assertValues(new Object[][] { { "x", 6L } }, r1);

    QueryResult r2 = query("select a1 as a, s.sb, s.cb from t10;");
    assertHeader(new String[] { "a", "sb", "cb" }, r2);
    assertValues(new Object[][] { { "x", 6L, 2L } }, r2);
  }

  @Test
  public void testReverseForeignKeyDependentTable() {
    execute("create table t12 (a1 varchar, constraint pk_a primary key (a1));");
    execute("create table t14 (a4 varchar, b4 integer, constraint pk_a primary key (a4));");
    execute("create table t13 (a2 varchar, a3 varchar,"
        + "constraint fk_v foreign key (a2) references t12(a1),"
        + "constraint fk_w foreign key (a3) references t14(a4));");
    execute("alter table t12 add view s (select sum(fk_w.b4) as sb from rev_fk_v);");

    execute("insert into t12 values ('x'), ('y');");
    execute("insert into t14 values ('a', 1), ('b', 5);");
    execute("insert into t13 values ('x', 'a'), ('x', 'a'), ('y', 'b');");

    QueryResult r1 = query("select a1, s.sb from t12;");
    assertHeader(new String[] { "a1", "sb" }, r1);
    assertValues(new Object[][] { { "x", 2L }, { "y", 5L } }, r1);
  }

  @Test
  public void testReverseForeignKeyWithWhere() {
    execute("create table t15 (a1 varchar, constraint pk_a primary key (a1));");
    execute("create table t16 (a2 varchar, b2 integer, c2 integer,"
        + "constraint fk_a foreign key (a2) references t15(a1));");
    execute("alter table t15 add view s (select sum(b2) as sb from rev_fk_a where c2 >= 2);");

    execute("insert into t15 values ('x');");
    execute("insert into t16 values ('x', 1, 1), ('x', 2, 2), ('x', 3, 3);");

    QueryResult r1 = query("select a1, s.sb from t15;");
    assertHeader(new String[] { "a1", "sb" }, r1);
    assertValues(new Object[][] { { "x", 5L } }, r1);
  }

  @Test
  public void testNullableForeignKey() {
    execute("create table t17 (a1 varchar, constraint pk_a primary key (a1));");
    execute("create table t18 (a2 varchar, b2 integer, constraint fk_a foreign key (a2) references t17(a1));");

    execute("insert into t17 values ('x');");
    execute("insert into t18 values ('x', 1);");
    execute("insert into t18 (b2) values (2);");

    QueryResult r1 = query("select a2, b2, fk_a.a1 from t18;");
    assertHeader(new String[] { "a2", "b2", "a1" }, r1);
    assertValues(new Object[][] {
        { "x", 1L, "x" },
        { null, 2L, null } }, r1);
  }

  @Test
  public void testNullableReverseForeignKey() {
    execute("create table t19 (a1 varchar, constraint pk_a primary key (a1));");
    execute("create table t20 (a2 varchar, b2 integer, constraint fk_a foreign key (a2) references t19(a1));");
    execute("alter table t19 add view s (select sum(b2) as sb from rev_fk_a);");

    execute("insert into t19 values ('x');");
    execute("insert into t20 values ('x', 1);");
    execute("insert into t20 (b2) values (2);");

    QueryResult r1 = query("select a1, s.sb from t19;");
    assertHeader(new String[] { "a1", "sb" }, r1);
    assertValues(new Object[][] { { "x", 1L } }, r1);
  }

  @Test
  public void testTableDepsInFuncArg() {
    execute("create table t21 (a varchar, b varchar, constraint pk_a primary key (a));");
    execute("create table t22 (a varchar, constraint fk_a foreign key (a) references t21);");

    execute("insert into t21 values ('x', 'abc');");
    execute("insert into t22 values ('x');");

    QueryResult r1 = query("select length(fk_a.b) as l from t22;");
    assertHeader(new String[] { "l" }, r1);
    assertValues(new Object[][] { { 3L } }, r1);
  }

  @Test
  public void testForeignKeyAsterisk() {
    execute("create table t23 (a varchar, b varchar, constraint pk_a primary key (a));");
    execute("create table t24 (a varchar, constraint fk_a foreign key (a) references t23);");

    execute("insert into t23 values ('x', 'y');");
    execute("insert into t24 values ('x');");

    QueryResult r1 = query("select fk_a.* from t24;");
    assertHeader(new String[] { "a", "b" }, r1);
    assertValues(new Object[][] { { "x", "y" } }, r1);
  }

  @Test
  public void testNameResolution() throws AuthException {
    execute("create user u1 identified by 'abc';");
    Session u1 = dbApi.authSession("u1", "abc", Method.LOCAL);
    u1.execute("create table t25 (a varchar);");
    u1.execute("insert into t25 values ('x');");

    ErrorResult e1 = error("select * from t25;");
    assertError(ModelException.class, "[14, 17]: Table or view 'admin.t25' does not exist.", e1);

    ErrorResult e2 = error("select * from u1.bad;");
    assertError(ModelException.class, "[17, 20]: Table 'u1.bad' does not exist.", e2);

    QueryResult r = query("select * from u1.t25;");
    assertValues(new Object[][] { { "x" } }, r);
  }
}
