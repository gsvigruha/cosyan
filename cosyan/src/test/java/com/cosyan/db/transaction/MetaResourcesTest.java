package com.cosyan.db.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.IParser.ParserException;
import com.cosyan.db.transaction.MetaResources.Resource;

public class MetaResourcesTest extends UnitTestBase {

  private TransactionHandler transactionHandler = new TransactionHandler();
  private Parser parser = new Parser();
  private Lexer lexer = new Lexer();

  private Map<String, Resource> resources(String sql)
      throws ModelException, ParserException, GrantException, IOException {
    DataTransaction transaction = transactionHandler
        .begin(parser.parseStatements(lexer.tokenize(sql)));
    Iterable<Resource> ress = transaction.collectResources(metaRepo).all();
    Map<String, Resource> map = new HashMap<>();
    for (Resource res : ress) {
      map.put(res.getResourceId(), res);
    }
    return map;
  }

  @Test
  public void testInsert() throws Exception {
    execute("create table t1 (a varchar);");
    Map<String, Resource> res = resources("insert into t1 values('x');");
    assertEquals(1, res.size());
    assertTrue(res.get("t1").isWrite());
  }

  @Test
  public void testDelete() throws Exception {
    execute("create table t2 (a varchar);");
    Map<String, Resource> res = resources("delete from t2 where a = 'x';");
    assertEquals(1, res.size());
    assertTrue(res.get("t2").isWrite());
  }

  @Test
  public void testUpdate() throws Exception {
    execute("create table t3 (a varchar);");
    Map<String, Resource> res = resources("update t3 set a = 'x';");
    assertEquals(1, res.size());
    assertTrue(res.get("t3").isWrite());
  }

  @Test
  public void testSelectViewForeignKey() throws Exception {
    execute("create table t4 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t5 (a varchar, constraint fk_a foreign key (a) references t4);");
    Map<String, Resource> res = resources("select fk_a.b from t5;");
    assertEquals(2, res.size());
    assertFalse(res.get("t4").isWrite());
    assertFalse(res.get("t5").isWrite());
  }

  @Test
  public void testSelectFromRef() throws Exception {
    execute("create table t6 (a varchar, constraint pk_a primary key (a));");
    execute("create table t7 (a varchar, b integer, constraint fk_a foreign key (a) references t6);");
    execute("alter table t6 add ref s (select sum(b) as b from rev_fk_a);");
    Map<String, Resource> res1 = resources("select a from t6;");
    assertEquals(1, res1.size());
    assertFalse(res1.get("t6").isWrite());

    Map<String, Resource> res2 = resources("select s.b from t6;");
    assertEquals(2, res2.size());
    assertFalse(res2.get("t6").isWrite());
    assertFalse(res2.get("t7").isWrite());
  }

  @Test
  public void testModifyWithRules() throws Exception {
    execute("create table t8 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t9 (a varchar, constraint fk_a foreign key (a) references t8,"
        + "constraint c_1 check (fk_a.b > 0));");
    Map<String, Resource> res1 = resources("insert into t8 values ('x', 1);");
    assertEquals(2, res1.size());
    assertTrue(res1.get("t8").isWrite());
    assertFalse(res1.get("t9").isWrite());

    Map<String, Resource> res2 = resources("delete from t8 where a = 'x';");
    assertEquals(2, res2.size());
    assertTrue(res2.get("t8").isWrite());
    assertFalse(res2.get("t9").isWrite());

    Map<String, Resource> res3 = resources("update t8 set b = 2;");
    assertEquals(2, res1.size());
    assertTrue(res3.get("t8").isWrite());
    assertFalse(res3.get("t9").isWrite());

    Map<String, Resource> res4 = resources("insert into t9 values ('x');");
    assertEquals(2, res4.size());
    assertTrue(res4.get("t9").isWrite());
    assertFalse(res4.get("t8").isWrite());

    Map<String, Resource> res5 = resources("delete from t9 where a = 'x';");
    assertEquals(1, res5.size());
    assertTrue(res5.get("t9").isWrite());

    Map<String, Resource> res6 = resources("update t9 set a = 'x';");
    assertEquals(2, res6.size());
    assertTrue(res6.get("t9").isWrite());
    assertFalse(res6.get("t8").isWrite());
  }
}
