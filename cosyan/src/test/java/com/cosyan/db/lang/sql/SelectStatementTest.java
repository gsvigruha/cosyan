package com.cosyan.db.lang.sql;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Result.QueryResult;

public class SelectStatementTest extends UnitTestBase {

  @Test
  public void testValuesFromDependentTable() {
    execute("create table t1 (a1 varchar, b1 integer, constraint pk_a primary key (a1));");
    execute("create table t2 (a2 varchar, b2 varchar, constraint fk_a foreign key (a2) references t1(a1));");
    execute("insert into t1 values ('x', 1), ('y', 2);");
    execute("insert into t2 values ('x', 'y'), ('x', 'z');");
    QueryResult result = query("select a2, b2, fk_a.a1, fk_a.b1 from t2;");
    assertHeader(new String[] { "a2", "b2", "a1", "b1" }, result);
    assertValues(new Object[][] {
        { "x", "y", "x", 1L },
        { "x", "z", "x", 1L } }, result);
  }

}
