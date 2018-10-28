package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.meta.View;
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
}
