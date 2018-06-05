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
import com.cosyan.db.model.Ident;
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
    dbApi.adminSession().execute("create table t1("
        + "a integer,"
        + "constraint pk_a primary key (a),"
        + "constraint c_a check(a > 1));");
    MaterializedTable tableMeta = dbApi.getMetaRepo().table("t1");
    assertEquals(1, tableMeta.columns().size());
    assertEquals(1, tableMeta.rules().size());
    assertEquals(true, tableMeta.primaryKey().isPresent());
    assertTrue(dbApi.getMetaRepo().collectUniqueIndexes(tableMeta).containsKey("a"));

    dbApi = new DBApi(config);
    MaterializedTable newTableMeta = dbApi.getMetaRepo().table("t1");
    assertEquals(tableMeta.columns(), newTableMeta.columns());
    assertEquals(tableMeta.rules().toString(), newTableMeta.rules().toString());
    assertEquals(tableMeta.primaryKey(), newTableMeta.primaryKey());
    assertEquals(tableMeta.foreignKeys(), newTableMeta.foreignKeys());
    assertEquals(tableMeta.reverseForeignKeys(), newTableMeta.reverseForeignKeys());
    assertTrue(dbApi.getMetaRepo().collectUniqueIndexes(newTableMeta).containsKey("a"));
  }

  @Test
  public void testTableReferencesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.adminSession().execute("create table t2("
        + "a integer,"
        + "constraint pk_a primary key (a));");
    dbApi.adminSession().execute("create table t3("
        + "a integer,"
        + "constraint pk_a primary key (a),"
        + "constraint fk_a1 foreign key (a) references t2(a));");
    dbApi.adminSession().execute("create table t4("
        + "a integer,"
        + "constraint fk_a2 foreign key (a) references t2(a));");
    MaterializedTable t2 = dbApi.getMetaRepo().table("t2");
    assertEquals(2, t2.reverseForeignKeys().size());
    MaterializedTable t3 = dbApi.getMetaRepo().table("t3");
    assertEquals(1, t3.foreignKeys().size());
    MaterializedTable t4 = dbApi.getMetaRepo().table("t4");
    assertEquals(1, t4.foreignKeys().size());
    dbApi = new DBApi(config);
    MaterializedTable newT2 = dbApi.getMetaRepo().table("t2");
    MaterializedTable newT3 = dbApi.getMetaRepo().table("t3");
    MaterializedTable newT4 = dbApi.getMetaRepo().table("t4");
    // Foreign keys doesn't have proper equals and hashcode to avoid infinite loops.
    assertEquals(t2.reverseForeignKeys().toString(), newT2.reverseForeignKeys().toString());
    assertEquals(t3.foreignKeys().toString(), newT3.foreignKeys().toString());
    assertEquals(t4.foreignKeys().toString(), newT4.foreignKeys().toString());
  }

  @Test
  public void testRulesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.adminSession().execute("create table t5("
        + "a integer,"
        + "constraint pk_a primary key (a));");
    dbApi.adminSession().execute("create table t6("
        + "a integer,"
        + "constraint fk_a1 foreign key (a) references t5(a),"
        + "constraint c_1 check(a = fk_a1.a));");
    MaterializedTable t5 = dbApi.getMetaRepo().table("t5");
    assertEquals(1, t5.reverseRuleDependencies().getDeps().size());
    MaterializedTable t6 = dbApi.getMetaRepo().table("t6");
    assertEquals(1, t6.rules().size());
    assertEquals(
        Iterables.getOnlyElement(t5.reverseRuleDependencies().getDeps().get("rev_fk_a1").rules()),
        t6.rules().get("c_1"));
    dbApi = new DBApi(config);
    MaterializedTable newT5 = dbApi.getMetaRepo().table("t5");
    assertEquals(1, newT5.reverseRuleDependencies().getDeps().size());
    MaterializedTable newT6 = dbApi.getMetaRepo().table("t6");
    assertEquals(1, newT6.rules().size());
    assertEquals(
        Iterables.getOnlyElement(newT5.reverseRuleDependencies().getDeps().get("rev_fk_a1").rules()),
        newT6.rules().get("c_1"));
  }

  @Test
  public void testRefRulesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.adminSession().execute("create table t7("
        + "a integer,"
        + "constraint pk_a primary key (a));");
    dbApi.adminSession().execute("create table t8("
        + "a integer,"
        + "constraint fk_a1 foreign key (a) references t7(a));");
    dbApi.adminSession().execute("alter table t7 add ref s (select sum(a) as sa from rev_fk_a1);");
    dbApi.adminSession().execute("alter table t7 add constraint c_1 check(s.sa < 10);");

    MaterializedTable t7 = dbApi.getMetaRepo().table("t7");
    assertEquals(1, t7.refs().size());
    assertEquals("rev_fk_a1", t7.refs().get("s").getTableMeta().getReverseForeignKey().getName());
    assertEquals(1, t7.rules().size());

    dbApi = new DBApi(config);
    MaterializedTable newT7 = dbApi.getMetaRepo().table("t7");
    assertEquals(1, newT7.refs().size());
    assertEquals("rev_fk_a1", newT7.refs().get("s").getTableMeta().getReverseForeignKey().getName());
    assertEquals(1, newT7.rules().size());

    dbApi.adminSession().execute("insert into t7 values (11);");
    ErrorResult e = (ErrorResult) dbApi.adminSession().execute("insert into t8 values (11);");
    assertEquals("Referencing constraint check t7.c_1 failed.", e.getError().getMessage());
  }

  @Test
  public void testDoubleRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.adminSession().execute("create table t9(a integer, b varchar);");

    MaterializedTable t9 = dbApi.getMetaRepo().table(new Ident("t9"));
    assertEquals(2, t9.columns().size());

    dbApi = new DBApi(config);
    MaterializedTable t9_2 = dbApi.getMetaRepo().table(new Ident("t9"));
    assertEquals(2, t9_2.columns().size());

    dbApi = new DBApi(config);
    MaterializedTable t9_3 = dbApi.getMetaRepo().table(new Ident("t9"));
    assertEquals(2, t9_3.columns().size());
  }

  @Test
  public void testTableDataAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.adminSession().execute("create table t10(a integer, b varchar);");
    dbApi.adminSession().execute("insert into t10 values(1, 'x');");

    dbApi = new DBApi(config);
    QueryResult result = query("select * from t10;", dbApi.adminSession());
    assertEquals(ImmutableList.of("a", "b"), result.getHeader());
    assertArrayEquals(new Object[] { 1L, "x" }, result.getValues().get(0));
  }

  @Test
  public void testIndexesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.adminSession().execute("create table t11(a integer unique);");
    dbApi.adminSession().execute("insert into t11 values(1);");

    dbApi = new DBApi(config);
    ErrorResult result = (ErrorResult) dbApi.adminSession().execute("insert into t11 values(1);");
    assertEquals("Key '1' already present in index.", result.getError().getMessage());
  }

  @Test
  public void testGeneratedIDAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.adminSession().execute("create table t12(a id, b varchar);");
    dbApi.adminSession().execute("insert into t12 values('x'), ('y');");
    QueryResult r1 = (QueryResult) ((TransactionResult) dbApi.adminSession().execute("select * from t12;"))
        .getResults().get(0);
    assertArrayEquals(new Object[] { 0L, "x" }, r1.getValues().get(0));
    assertArrayEquals(new Object[] { 1L, "y" }, r1.getValues().get(1));

    dbApi = new DBApi(config);
    dbApi.adminSession().execute("insert into t12 values('z');");
    QueryResult r2 = query("select * from t12;", dbApi.adminSession());

    assertArrayEquals(new Object[] { 0L, "x" }, r2.getValues().get(0));
    assertArrayEquals(new Object[] { 1L, "y" }, r2.getValues().get(1));
    assertArrayEquals(new Object[] { 2L, "z" }, r2.getValues().get(2));
  }

  @Test
  public void testUsersAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.adminSession().execute("create table t13(a integer);");
    dbApi.adminSession().execute("insert into t13 values(1);");
    dbApi.adminSession().execute("create user u1 identified by 'abc';");
    dbApi.adminSession().execute("create user u2 identified by 'abc';");

    Session u1 = dbApi.authSession("u1", "abc", Method.LOCAL);
    ErrorResult e1 = (ErrorResult) u1.execute("select * from t13;");
    assertEquals("User 'u1' has no SELECT right on 't13'.", e1.getError().getMessage());

    dbApi.adminSession().execute("grant select on t13 to u1;");
    QueryResult r1 = query("select * from t13;", u1);
    assertArrayEquals(new Object[] { 1L }, r1.getValues().get(0));

    Session u2 = dbApi.authSession("u2", "abc", Method.LOCAL);
    ErrorResult e2 = (ErrorResult) u2.execute("select * from t13;");
    assertEquals("User 'u2' has no SELECT right on 't13'.", e2.getError().getMessage());

    dbApi.adminSession().execute("grant all on * to u2;");
    QueryResult r2 = query("select * from t13;", u2);
    assertArrayEquals(new Object[] { 1L }, r2.getValues().get(0));

    dbApi = new DBApi(config);
    Session u1_2 = dbApi.authSession("u1", "abc", Method.LOCAL);
    QueryResult r1_2 = query("select * from t13;", u1_2);
    assertArrayEquals(new Object[] { 1L }, r1_2.getValues().get(0));

    Session u2_2 = dbApi.authSession("u2", "abc", Method.LOCAL);
    QueryResult r2_2 = query("select * from t13;", u2_2);
    assertArrayEquals(new Object[] { 1L }, r2_2.getValues().get(0));
  }

  @Test
  public void testIndexesCreatedWithDataAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.adminSession().execute("create table t14(a integer);");
    dbApi.adminSession().execute("insert into t14 values (1), (2);");
    dbApi.adminSession().execute("create index t14.a;");
    {
      MaterializedTable t14 = dbApi.getMetaRepo().table("t14");
      IndexReader index = dbApi.getMetaRepo().collectIndexReaders(t14).get("a");
      assertArrayEquals(new long[] { 0L }, index.get(1L));
      assertArrayEquals(new long[] { 18L }, index.get(2L));
    }

    dbApi = new DBApi(config);
    {
      MaterializedTable t14 = dbApi.getMetaRepo().table("t14");
      IndexReader index = dbApi.getMetaRepo().collectIndexReaders(t14).get("a");
      assertArrayEquals(new long[] { 0L }, index.get(1L));
      assertArrayEquals(new long[] { 18L }, index.get(2L));
    }
  }

  @Test
  public void testIDIndexesAfterRestart() throws Exception {
    DBApi dbApi = new DBApi(config);
    dbApi.adminSession().execute("create table t15(a id, b varchar);");
    dbApi.adminSession().execute("insert into t15 values ('x');");
    dbApi.adminSession().execute("insert into t15 values ('y');");
    {
      MaterializedTable t15 = dbApi.getMetaRepo().table("t15");
      IndexReader index = dbApi.getMetaRepo().collectIndexReaders(t15).get("a");
      assertArrayEquals(new long[] { 0L }, index.get(0L));
      assertArrayEquals(new long[] { 25L }, index.get(1L));
    }

    dbApi = new DBApi(config);
    {
      MaterializedTable t15 = dbApi.getMetaRepo().table("t15");
      IndexReader index = dbApi.getMetaRepo().collectIndexReaders(t15).get("a");
      assertArrayEquals(new long[] { 0L }, index.get(0L));
      assertArrayEquals(new long[] { 25L }, index.get(1L));
    }
  }
}
