package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.Rule;

public class DependenciesTest extends UnitTestBase {

  @Test
  public void testCreateReferencingRule() throws Exception {
    execute("create table t1 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t2 (a varchar,"
        + "constraint fk_a foreign key (a) references t1(a),"
        + "constraint c_b check (fk_a.b > 1));");
    MaterializedTableMeta t2 = metaRepo.table(new Ident("t2"));
    Rule rule = t2.rules().get("c_b");
    assertEquals("c_b", rule.getName());

    assertEquals(1, t2.ruleDependencies().size());
    assertEquals(0, t2.reverseRuleDependencies().getDeps().size());
    assertEquals("fk_a", t2.ruleDependencies().get("fk_a").getForeignKey().getName());

    MaterializedTableMeta t1 = metaRepo.table(new Ident("t1"));
    assertEquals(0, t1.ruleDependencies().size());
    assertEquals(1, t1.reverseRuleDependencies().getDeps().size());
    assertEquals(1, t1.reverseRuleDependencies().getDeps().size());
    assertEquals("rev_fk_a", t1.reverseRuleDependencies().getDeps().get("rev_fk_a").getKey().getName());
    assertEquals(1, t1.reverseRuleDependencies().getDeps().get("rev_fk_a").getRules().size());
    assertEquals("c_b", t1.reverseRuleDependencies().getDeps().get("rev_fk_a").getRules().get("c_b").getName());
  }
}
