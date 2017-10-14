package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.model.ColumnMeta.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.sql.Result.ErrorResult;
import com.cosyan.db.sql.Result.QueryResult;
import com.cosyan.db.sql.SyntaxTree.Ident;

public class AlterStatementTest extends UnitTestBase {

  @Test
  public void testDropColumn() throws Exception {
    execute("create table t1 (a varchar, b integer, c float);");
    execute("alter table t1 drop b;");
    MaterializedTableMeta tableMeta = metaRepo.table(new Ident("t1"));
    assertEquals(2, tableMeta.columns().size());
    assertEquals(new BasicColumn(0, "a", DataTypes.StringType, true, false),
        tableMeta.column(new Ident("a")));
    assertEquals(new BasicColumn(2, "c", DataTypes.DoubleType, true, false),
        tableMeta.column(new Ident("c")));
  }

  @Test
  public void testDropColumnTestData() throws Exception {
    execute("create table t2 (a varchar, b integer, c float);");
    execute("insert into t2 values('x', 1, 1.0);");
    execute("insert into t2 values('y', 2, 2.0);");
    execute("alter table t2 drop b;");
    {
      QueryResult result = query("select * from t2;");
      assertHeader(new String[] { "a", "c" }, result);
      assertValues(new Object[][] { { "x", 1.0 }, { "y", 2.0 } }, result);
    }

    execute("insert into t2 values('z', 3.0);");
    {
      QueryResult result = query("select * from t2;");
      assertHeader(new String[] { "a", "c" }, result);
      assertValues(new Object[][] { { "x", 1.0 }, { "y", 2.0 }, { "z", 3.0 } }, result);
    }
  }

  @Test
  public void testDropColumnWithConstraints() throws Exception {
    execute("create table t3 (a integer, b integer unique, constraint c_a check(a > 1));");
    {
      ErrorResult result = error("alter table t3 drop a;");
      assertEquals("Cannot drop column 'a', check 'c_a [(a > 1)]' fails.\n" +
          "Column 'a' does not exist.", result.getError().getMessage());
    }
    execute("create table t4 (a integer, constraint fk_a foreign key (a) references t3(b));");
    {
      ErrorResult result = error("alter table t4 drop a;");
      assertEquals("Cannot drop column 'a', it is used by foreign key 'fk_a [a -> t3.b]'.",
          result.getError().getMessage());
    }
    {
      ErrorResult result = error("alter table t3 drop b;");
      assertEquals("Cannot drop column 'b', it is referenced by foreign key 'fk_a [t4.a -> b]'.",
          result.getError().getMessage());
    }
  }
}
