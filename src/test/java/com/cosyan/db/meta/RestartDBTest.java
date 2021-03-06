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
package com.cosyan.db.meta;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.DBApi;
import com.cosyan.db.auth.Authenticator.Method;
import com.cosyan.db.conf.Config;
import com.cosyan.db.conf.Config.ConfigException;
import com.cosyan.db.io.Indexes.IndexReader;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.CrashResult;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.lang.transaction.Result.TransactionResult;
import com.cosyan.db.meta.View.TopLevelView;
import com.cosyan.db.session.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class RestartDBTest {

  private static Config config;

  @BeforeClass
  public static void before() throws IOException, ConfigException {
    FileUtils.cleanDirectory(new File("/tmp/data"));
    FileUtils.copyFile(new File("src/test/resources/cosyan.db.properties"), new File("/tmp/data/cosyan.db.properties"));
    FileUtils.copyFile(new File("conf/users"), new File("/tmp/data/users"));
    config = new Config("/tmp/data");
  }

  private QueryResult query(String sql, Session session) {
    Result result = session.execute(sql);
    if (result instanceof ErrorResult) {
      ((ErrorResult) result).getError().printStackTrace();
      fail(sql);
    }
    if (result instanceof CrashResult) {
      ((CrashResult) result).getError().printStackTrace();
      fail(sql);
    }
    return (QueryResult) ((TransactionResult) result).getResults().get(0);
  }

  @Test
  public void testTablesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t1("
        + "a integer,"
        + "constraint pk_a primary key (a),"
        + "constraint c_a check(a > 1));");
    MaterializedTable tableMeta = dbApi.getMetaRepo().table("admin", "t1");
    assertEquals(1, tableMeta.columns().size());
    assertEquals(1, tableMeta.rules().size());
    assertEquals(true, tableMeta.primaryKey().isPresent());
    assertTrue(tableMeta.uniqueIndexes().containsKey("a"));

    dbApi = new DBApi(config);
    MaterializedTable newTableMeta = dbApi.getMetaRepo().table("admin", "t1");
    assertEquals(tableMeta.columns(), newTableMeta.columns());
    assertEquals(tableMeta.rules().toString(), newTableMeta.rules().toString());
    assertEquals(tableMeta.primaryKey(), newTableMeta.primaryKey());
    assertEquals(tableMeta.foreignKeys(), newTableMeta.foreignKeys());
    assertEquals(tableMeta.reverseForeignKeys(), newTableMeta.reverseForeignKeys());
    assertTrue(newTableMeta.uniqueIndexes().containsKey("a"));
  }

  @Test
  public void testTableReferencesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t2("
        + "a integer,"
        + "constraint pk_a primary key (a));");
    dbApi.newAdminSession().execute("create table t3("
        + "a integer,"
        + "constraint pk_a primary key (a),"
        + "constraint fk_a1 foreign key (a) references t2(a));");
    dbApi.newAdminSession().execute("create table t4("
        + "a integer,"
        + "constraint fk_a2 foreign key (a) references t2(a));");
    MaterializedTable t2 = dbApi.getMetaRepo().table("admin", "t2");
    assertEquals(2, t2.reverseForeignKeys().size());
    MaterializedTable t3 = dbApi.getMetaRepo().table("admin", "t3");
    assertEquals(1, t3.foreignKeys().size());
    MaterializedTable t4 = dbApi.getMetaRepo().table("admin", "t4");
    assertEquals(1, t4.foreignKeys().size());
    dbApi = new DBApi(config);
    MaterializedTable newT2 = dbApi.getMetaRepo().table("admin", "t2");
    MaterializedTable newT3 = dbApi.getMetaRepo().table("admin", "t3");
    MaterializedTable newT4 = dbApi.getMetaRepo().table("admin", "t4");
    // Foreign keys doesn't have proper equals and hashcode to avoid infinite loops.
    assertEquals(t2.reverseForeignKeys().toString(), newT2.reverseForeignKeys().toString());
    assertEquals(t3.foreignKeys().toString(), newT3.foreignKeys().toString());
    assertEquals(t4.foreignKeys().toString(), newT4.foreignKeys().toString());
  }

  @Test
  public void testRulesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t5("
        + "a integer,"
        + "constraint pk_a primary key (a));");
    dbApi.newAdminSession().execute("create table t6("
        + "a integer,"
        + "constraint fk_a1 foreign key (a) references t5(a),"
        + "constraint c_1 check(a = fk_a1.a));");
    MaterializedTable t5 = dbApi.getMetaRepo().table("admin", "t5");
    assertEquals(1, t5.reverseRuleDependencies().getDeps().size());
    MaterializedTable t6 = dbApi.getMetaRepo().table("admin", "t6");
    assertEquals(1, t6.rules().size());
    assertEquals(
        Iterables.getOnlyElement(t5.reverseRuleDependencies().getDeps().get("rev_fk_a1").rules()),
        t6.rules().get("c_1"));
    dbApi = new DBApi(config);
    MaterializedTable newT5 = dbApi.getMetaRepo().table("admin", "t5");
    assertEquals(1, newT5.reverseRuleDependencies().getDeps().size());
    MaterializedTable newT6 = dbApi.getMetaRepo().table("admin", "t6");
    assertEquals(1, newT6.rules().size());
    assertEquals(
        Iterables.getOnlyElement(newT5.reverseRuleDependencies().getDeps().get("rev_fk_a1").rules()),
        newT6.rules().get("c_1"));
  }

  @Test
  public void testRefRulesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t7("
        + "a integer,"
        + "constraint pk_a primary key (a));");
    dbApi.newAdminSession().execute("create table t8("
        + "a integer,"
        + "constraint fk_a1 foreign key (a) references t7(a));");
    dbApi.newAdminSession().execute("alter table t7 add view s (select sum(a) as sa from rev_fk_a1);");
    dbApi.newAdminSession().execute("alter table t7 add constraint c_1 check(s.sa < 10);");

    MaterializedTable t7 = dbApi.getMetaRepo().table("admin", "t7");
    assertEquals(1, t7.refs().size());
    assertEquals(ImmutableList.of("sa"), t7.refs().get("s").getTableMeta().columnNames());
    assertEquals(1, t7.rules().size());

    dbApi = new DBApi(config);
    MaterializedTable newT7 = dbApi.getMetaRepo().table("admin", "t7");
    assertEquals(1, newT7.refs().size());
    assertEquals(ImmutableList.of("sa"), newT7.refs().get("s").getTableMeta().columnNames());
    assertEquals(1, newT7.rules().size());

    dbApi.newAdminSession().execute("insert into t7 values (11);");
    ErrorResult e = (ErrorResult) dbApi.newAdminSession().execute("insert into t8 values (11);");
    assertEquals("Referencing constraint check t7.c_1 failed.", e.getError().getMessage());
  }

  @Test
  public void testDoubleRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t9(a integer, b varchar);");

    MaterializedTable t9 = dbApi.getMetaRepo().table("admin", "t9");
    assertEquals(2, t9.columns().size());

    dbApi = new DBApi(config);
    MaterializedTable t9_2 = dbApi.getMetaRepo().table("admin", "t9");
    assertEquals(2, t9_2.columns().size());

    dbApi = new DBApi(config);
    MaterializedTable t9_3 = dbApi.getMetaRepo().table("admin", "t9");
    assertEquals(2, t9_3.columns().size());
  }

  @Test
  public void testTableDataAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t10(a integer, b varchar);");
    dbApi.newAdminSession().execute("insert into t10 values(1, 'x');");

    dbApi = new DBApi(config);
    QueryResult result = query("select * from t10;", dbApi.newAdminSession());
    assertEquals(ImmutableList.of("a", "b"), result.getHeader());
    assertArrayEquals(new Object[] { 1L, "x" }, result.getValues().get(0));
  }

  @Test
  public void testIndexesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t11(a integer unique);");
    dbApi.newAdminSession().execute("insert into t11 values(1);");

    dbApi = new DBApi(config);
    ErrorResult result = (ErrorResult) dbApi.newAdminSession().execute("insert into t11 values(1);");
    assertEquals("Key '1' already present in index.", result.getError().getMessage());
  }

  @Test
  public void testGeneratedIDAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t12(a id, b varchar);");
    dbApi.newAdminSession().execute("insert into t12 values('x'), ('y');");
    QueryResult r1 = (QueryResult) ((TransactionResult) dbApi.newAdminSession().execute("select * from t12;"))
        .getResults().get(0);
    assertArrayEquals(new Object[] { 0L, "x" }, r1.getValues().get(0));
    assertArrayEquals(new Object[] { 1L, "y" }, r1.getValues().get(1));

    dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("insert into t12 values('z');");
    QueryResult r2 = query("select * from t12;", dbApi.newAdminSession());

    assertArrayEquals(new Object[] { 0L, "x" }, r2.getValues().get(0));
    assertArrayEquals(new Object[] { 1L, "y" }, r2.getValues().get(1));
    assertArrayEquals(new Object[] { 2L, "z" }, r2.getValues().get(2));
  }

  @Test
  public void testUsersAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t13(a integer);");
    dbApi.newAdminSession().execute("insert into t13 values(1);");
    dbApi.newAdminSession().execute("create user u1 identified by 'abc';");
    dbApi.newAdminSession().execute("create user u2 identified by 'abc';");

    Session u1 = dbApi.authSession("u1", "abc", Method.LOCAL);
    ErrorResult e1 = (ErrorResult) u1.execute("select * from admin.t13;");
    assertEquals("User 'u1' has no SELECT right on 'admin.t13'.", e1.getError().getMessage());

    dbApi.newAdminSession().execute("grant select on t13 to u1;");
    QueryResult r1 = query("select * from admin.t13;", u1);
    assertArrayEquals(new Object[] { 1L }, r1.getValues().get(0));

    Session u2 = dbApi.authSession("u2", "abc", Method.LOCAL);
    ErrorResult e2 = (ErrorResult) u2.execute("select * from admin.t13;");
    assertEquals("User 'u2' has no SELECT right on 'admin.t13'.", e2.getError().getMessage());

    dbApi.newAdminSession().execute("grant all on * to u2;");
    QueryResult r2 = query("select * from admin.t13;", u2);
    assertArrayEquals(new Object[] { 1L }, r2.getValues().get(0));

    dbApi = new DBApi(config);
    Session u1_2 = dbApi.authSession("u1", "abc", Method.LOCAL);
    QueryResult r1_2 = query("select * from admin.t13;", u1_2);
    assertArrayEquals(new Object[] { 1L }, r1_2.getValues().get(0));

    Session u2_2 = dbApi.authSession("u2", "abc", Method.LOCAL);
    QueryResult r2_2 = query("select * from admin.t13;", u2_2);
    assertArrayEquals(new Object[] { 1L }, r2_2.getValues().get(0));
  }

  @Test
  public void testIndexesCreatedWithDataAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t14(a integer);");
    dbApi.newAdminSession().execute("insert into t14 values (1), (2);");
    dbApi.newAdminSession().execute("create index t14.a;");
    {
      MaterializedTable t14 = dbApi.getMetaRepo().table("admin", "t14");
      IndexReader index = t14.allIndexReaders().get("a");
      assertArrayEquals(new long[] { 0L }, index.get(1L));
      assertArrayEquals(new long[] { 18L }, index.get(2L));
    }

    dbApi = new DBApi(config);
    {
      MaterializedTable t14 = dbApi.getMetaRepo().table("admin", "t14");
      IndexReader index = t14.allIndexReaders().get("a");
      assertArrayEquals(new long[] { 0L }, index.get(1L));
      assertArrayEquals(new long[] { 18L }, index.get(2L));
    }
  }

  @Test
  public void testIDIndexesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t15(a id, b varchar);");
    dbApi.newAdminSession().execute("insert into t15 values ('x');");
    dbApi.newAdminSession().execute("insert into t15 values ('y');");
    {
      MaterializedTable t15 = dbApi.getMetaRepo().table("admin", "t15");
      IndexReader index = t15.allIndexReaders().get("a");
      assertArrayEquals(new long[] { 0L }, index.get(0L));
      assertArrayEquals(new long[] { 25L }, index.get(1L));
    }

    dbApi = new DBApi(config);
    {
      MaterializedTable t15 = dbApi.getMetaRepo().table("admin", "t15");
      IndexReader index = t15.allIndexReaders().get("a");
      assertArrayEquals(new long[] { 0L }, index.get(0L));
      assertArrayEquals(new long[] { 25L }, index.get(1L));
    }
  }

  @Test
  public void testAggrefInAggrefAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t18(a id);");
    dbApi.newAdminSession().execute("create table t17 (a id, b integer, constraint fk1 foreign key (b) references t18);");
    dbApi.newAdminSession().execute("create table t16 (a integer, constraint fk2 foreign key (a) references t17);");
    dbApi.newAdminSession().execute("alter table t17 add view s (select count(1) as c from rev_fk2);");
    dbApi.newAdminSession().execute("alter table t18 add view s (select sum(s.c) as s from rev_fk1);");

    {
      MaterializedTable t18 = dbApi.getMetaRepo().table("admin", "t18");
      assertEquals("select sum(s.c) as s from rev_fk1 ", t18.refs().get("s").getExpr());
    }

    dbApi = new DBApi(config);
    {
      MaterializedTable t18 = dbApi.getMetaRepo().table("admin", "t18");
      assertEquals("select sum(s.c) as s from rev_fk1 ;", t18.refs().get("s").getExpr());
    }
  }

  @Test
  public void testFlatRefAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t19(a id, b integer);");
    dbApi.newAdminSession().execute("create table t20 (a id, b integer, constraint fk1 foreign key (b) references t19);");
    dbApi.newAdminSession().execute("alter table t20 add view s (select fk1.b + 1 as x, fk1.b + 2 as y from t20);");

    {
      MaterializedTable t20 = dbApi.getMetaRepo().table("admin", "t20");
      assertEquals("select (fk1.b + 1) as x, (fk1.b + 2) as y from t20 ", t20.refs().get("s").getExpr());
    }

    dbApi = new DBApi(config);
    {
      MaterializedTable t20 = dbApi.getMetaRepo().table("admin", "t20");
      assertEquals("select (fk1.b + 1) as x, (fk1.b + 2) as y from t20 ;", t20.refs().get("s").getExpr());
    }
  }

  @Test
  public void testViews() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.newAdminSession().execute("create table t21(a integer, b integer);");
    dbApi.newAdminSession().execute("create view v22 as select b, sum(a) as a from t21 group by b;");
    dbApi.newAdminSession().execute("alter view v22 add constraint c_1 check(a < 5);");
    dbApi.newAdminSession().execute("insert into t21 values (1, 1), (1, 1);");

    {
      TopLevelView v22 = dbApi.getMetaRepo().view("admin", "v22");
      assertEquals("admin.v22", v22.fullName());
      QueryResult r = query("select * from v22;", dbApi.newAdminSession());
      assertArrayEquals(new Object[] { 1L, 2L }, r.getValues().get(0));
      assertEquals("(a < 5)", v22.rules().get("c_1").getExpr().print());
      ErrorResult e = (ErrorResult) dbApi.newAdminSession().execute("insert into t21 values (3, 1);");
      assertEquals("Referencing constraint check v22.c_1 failed.", e.getError().getMessage());
    }

    dbApi = new DBApi(config);
    {
      TopLevelView v22 = dbApi.getMetaRepo().view("admin", "v22");
      assertEquals("admin.v22", v22.fullName());
      QueryResult r = query("select * from v22;", dbApi.newAdminSession());
      assertArrayEquals(new Object[] { 1L, 2L }, r.getValues().get(0));
      assertEquals("(a < 5)", v22.rules().get("c_1").getExpr().print());
      ErrorResult e = (ErrorResult) dbApi.newAdminSession().execute("insert into t21 values (3, 1);");
      assertEquals("Referencing constraint check v22.c_1 failed.", e.getError().getMessage());
    }
  }
}
