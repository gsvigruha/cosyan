package com.cosyan.db.transaction;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.transaction.MetaResources.Resource;

public class MetaResourcesTest extends UnitTestBase {

  private TransactionHandler transactionHandler = new TransactionHandler();
  private Parser parser = new Parser();
  private Lexer lexer = new Lexer();

  private Map<String, Resource> resources(String sql) throws ModelException, ParserException {
    Transaction transaction = transactionHandler
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
    assertEquals(4, res.size());
    assertFalse(res.get("t4").isWrite());
    assertFalse(res.get("t4.a").isWrite());
    assertFalse(res.get("t5").isWrite());
    assertFalse(res.get("t5.a").isWrite());
  }

  @Test
  public void testSelectFromRef() throws Exception {
    execute("create table t6 (a varchar, constraint pk_a primary key (a));");
    execute("create table t7 (a varchar, b integer, constraint fk_a foreign key (a) references t6);");
    execute("alter table t6 add ref s (select sum(b) as b from rev_fk_a);");
    Map<String, Resource> res1 = resources("select a from t6;");
    assertEquals(2, res1.size());
    assertFalse(res1.get("t6").isWrite());
    assertFalse(res1.get("t6.a").isWrite());

    Map<String, Resource> res2 = resources("select s.b from t6;");
    assertEquals(4, res2.size());
    assertFalse(res2.get("t6").isWrite());
    assertFalse(res2.get("t6.a").isWrite());
    assertFalse(res2.get("t7").isWrite());
    assertFalse(res2.get("t7.a").isWrite());
  }
}
