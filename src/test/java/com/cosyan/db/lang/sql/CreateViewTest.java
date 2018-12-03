package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.meta.View;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.google.common.collect.ImmutableList;

public class CreateViewTest extends UnitTestBase {

  @Test
  public void testCreateView() throws Exception {
    execute("create table t1 (a varchar not null, b integer, c float);");
    execute("create view v1 as select a, sum(b) as b, max(c) as c from t1 group by a;");
    View view = metaRepo.view("admin", "v1");
    assertEquals("v1", view.name());
    assertEquals("admin", view.owner());
    assertEquals(ImmutableList.of("a", "b", "c"), view.table().columnNames());
  }

  @Test
  public void testSelectFromView() throws Exception {
    execute("create table t2 (a varchar not null, b integer, c float);");
    execute("create view v2 as select a, sum(b) as b, max(c) as c from t2 group by a;");
    execute("insert into t2 values ('x', 2, 5.0), ('x', 2, 7.0);");

    {
      QueryResult r = query("select * from v2;");
      assertHeader(new String[] { "a", "b", "c" }, r);
      assertValues(new Object[][] { { "x", 4L, 7.0 } }, r);
    }

    execute("insert into t2 values ('y', 3, 4.0);");
    {
      QueryResult r = query("select * from v2;");
      assertHeader(new String[] { "a", "b", "c" }, r);
      assertValues(new Object[][] { { "x", 4L, 7.0 }, { "y", 3L, 4.0 } }, r);
    }
  }

  @Test
  public void testViewRules() throws Exception {
    execute("create table t3 (a varchar not null, b integer);");
    execute("create view v3 as select a, sum(b) as b from t3 group by a;");
    execute("alter view v3 add constraint c_1 check(b < 5);");
    execute("insert into t3 values ('x', 2), ('x', 2);");

    QueryResult r = query("select * from v3;");
    assertHeader(new String[] { "a", "b" }, r);
    assertValues(new Object[][] { { "x", 4L } }, r);

    ErrorResult e = error("insert into t3 values ('x', 1);");
    assertError(RuleException.class, "Referencing constraint check v3.c_1 failed.", e);
  }

  @Test
  public void testViewRulesExprAggr() throws Exception {
    execute("create table t4 (a varchar not null, b integer);");
    execute("create view v4 as select l, sum(b) as b from t4 group by length(a) as l;");
    execute("alter view v4 add constraint c_1 check(b < 5);");
    execute("insert into t4 values ('x', 2), ('y', 2), ('zz', 3);");

    QueryResult r1 = query("select * from v4;");
    assertHeader(new String[] { "l", "b" }, r1);
    assertValues(new Object[][] { { 1L, 4L }, { 2L, 3L } }, r1);

    ErrorResult e1 = error("insert into t4 values ('z', 1);");
    assertError(RuleException.class, "Referencing constraint check v4.c_1 failed.", e1);

    ErrorResult e2 = error("insert into t4 values ('ww', 2);");
    assertError(RuleException.class, "Referencing constraint check v4.c_1 failed.", e2);

    execute("insert into t4 values ('ww', 1);");
    QueryResult r2 = query("select * from v4;");
    assertHeader(new String[] { "l", "b" }, r2);
    assertValues(new Object[][] { { 1L, 4L }, { 2L, 4L } }, r2);
  }

  @Test
  public void testRefInView() throws Exception {
    execute("create table t5 (a id, b integer);");
    execute("create table t6 (a integer, c varchar, constraint fk_a foreign key(a) references t5);");
    execute("create view v5 as select c, sum(fk_a.b) as b from t6 group by c;");
    execute("insert into t5 values (2), (5);");
    execute("insert into t6 values (0, 'x'), (1, 'x');");

    QueryResult r = query("select * from v5;");
    assertHeader(new String[] { "c", "b" }, r);
    assertValues(new Object[][] { { "x", 7L } }, r);

    execute("alter view v5 add constraint c_1 check(b < 8);");
    ErrorResult e1 = error("update t5 set b=3 where a=0;");
    assertError(RuleException.class, "Referencing constraint check v5.c_1 failed.", e1);
  }

  @Test
  public void testViewInView() throws Exception {
    execute("create table t7 (a integer);");
    execute("create view v8 as select a + 1 as b from t7;");
    ErrorResult e = error("create view v9 as select b + 1 as c from v8;");
    assertError(ModelException.class, "[12, 14]: Unsupported table 'v8' for view.", e);
  }
}
