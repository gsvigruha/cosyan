package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Result.ErrorResult;
import com.cosyan.db.lang.sql.Result.QueryResult;
import com.cosyan.db.lang.sql.SyntaxTree.Ident;
import com.cosyan.db.meta.MetaRepo.ModelException;

public class DropStatementTest extends UnitTestBase {

  @Test
  public void testDropTable() throws Exception {
    execute("create table t1 (a varchar);");
    metaRepo.table(new Ident("t1"));
    execute("drop table t1;");
    try {
      metaRepo.table(new Ident("t1"));
      fail();
    } catch (ModelException e) {
      assertEquals("Table 't1' does not exist.", e.getMessage());
    }
  }

  @Test
  public void testQueryDroppedTable() throws Exception {
    execute("create table t2 (a varchar);");
    execute("insert into t2 values('x');");
    QueryResult result = query("select * from t2;");
    assertHeader(new String[] { "a" }, result);
    assertValues(new Object[][] { { "x" } }, result);

    execute("drop table t2;");
    ErrorResult e = error("select * from t2;");
    assertEquals("Table 't2' does not exist.", e.getError().getMessage());
  }

  @Test
  public void testCanNotDropTableWithReference() throws Exception {
    execute("create table t3 (a varchar unique);");
    execute("create table t4 (a varchar, constraint fk_a foreign key (a) references t3(a));");

    ErrorResult e = error("drop table t3;");
    assertEquals("Cannot drop table 't3', referenced by foreign key 't4.fk_a [t4.a -> a]'.", e.getError().getMessage());
  }

  @Test
  public void testDropIndex() throws Exception {
    execute("create table t5 (a varchar);");
    execute("create index t5.a;");
    assertEquals(1, metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t5"))).size());
    execute("drop index t5.a;");
    assertEquals(0, metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t5"))).size());
  }

  @Test
  public void testDropIndexError() throws Exception {
    execute("create table t6 (a varchar unique, b varchar);");

    {
      ErrorResult e = error("drop index t6.a;");
      assertEquals("Cannot drop index 't6.a', column is unique.", e.getError().getMessage());
    }
    {
      ErrorResult e = error("drop index t6.b;");
      assertEquals("Cannot drop index 't6.b', column is not indexed.", e.getError().getMessage());
    }
    {
      ErrorResult e = error("drop index t6.c;");
      assertEquals("Column 'c' not found in table.", e.getError().getMessage());
    }
  }
}
