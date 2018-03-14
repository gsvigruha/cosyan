package com.cosyan.db.lang.sql;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.Dependencies.TableDependencies;

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
    assertEquals("fk_a", t2.ruleDependencies().get("fk_a").getRef().getName());

    MaterializedTableMeta t1 = metaRepo.table(new Ident("t1"));
    assertEquals(0, t1.ruleDependencies().size());
    assertEquals(1, t1.reverseRuleDependencies().getDeps().size());
    assertEquals(1, t1.reverseRuleDependencies().getDeps().size());
    assertEquals("rev_fk_a", t1.reverseRuleDependencies().getDeps().get("rev_fk_a").getKey().getName());
    assertEquals(1, t1.reverseRuleDependencies().getDeps().get("rev_fk_a").getRules().size());
    assertEquals("c_b", t1.reverseRuleDependencies().getDeps().get("rev_fk_a").getRules().get("c_b").getName());
  }

  @Test
  public void testCreateReferencingRule_GlobalDependency() throws Exception {
    execute("create table t3 (a varchar, constraint pk_a primary key (a));");
    execute("create table t4 (a varchar, constraint fk_a foreign key (a) references t3(a));");
    // The rule does not reference any columns.
    execute("alter table t3 add ref s (select count(1) as c from rev_fk_a);");
    execute("alter table t3 add constraint c_1 check (s.c <= 2);");

    MaterializedTableMeta t3 = metaRepo.table(new Ident("t3"));
    Rule rule = t3.rules().get("c_1");
    TableDependencies deps = rule.getColumn().tableDependencies();
    assertEquals(1, deps.getDeps().size());
    assertEquals(0, deps.getDeps().get("rev_fk_a").getDeps().size());

    assertEquals(1, t3.ruleDependencies().size());
    assertEquals(0, t3.reverseRuleDependencies().getDeps().size());
    assertEquals("rev_fk_a", t3.ruleDependencies().get("rev_fk_a").getRef().getName());

    MaterializedTableMeta t4 = metaRepo.table(new Ident("t4"));
    assertEquals(0, t4.ruleDependencies().size());
    assertEquals(1, t4.reverseRuleDependencies().getDeps().size());
    assertEquals("fk_a", t4.reverseRuleDependencies().getDeps().get("fk_a").getKey().getName());
  }

  @Test
  public void testCreateReferencingRule_MultipleKeys() throws Exception {
    execute("create table t5 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t6 (a varchar, "
        + "constraint fk_a_1 foreign key (a) references t5(a),"
        + "constraint fk_a_2 foreign key (a) references t5(a),"
        + "constraint c check (fk_a_1.b > fk_a_2.b));");

    MaterializedTableMeta t6 = metaRepo.table(new Ident("t6"));
    Rule rule = t6.rules().get("c");
    TableDependencies deps = rule.getColumn().tableDependencies();
    assertEquals(2, deps.getDeps().size());
    assertEquals(0, deps.getDeps().get("fk_a_1").getDeps().size());
    assertEquals(0, deps.getDeps().get("fk_a_2").getDeps().size());

    assertEquals(2, t6.ruleDependencies().size());
    assertEquals(0, t6.reverseRuleDependencies().getDeps().size());
    assertEquals("fk_a_1", t6.ruleDependencies().get("fk_a_1").getRef().getName());
    assertEquals("fk_a_2", t6.ruleDependencies().get("fk_a_2").getRef().getName());

    MaterializedTableMeta t5 = metaRepo.table(new Ident("t5"));
    assertEquals(0, t5.ruleDependencies().size());
    assertEquals(2, t5.reverseRuleDependencies().getDeps().size());
    assertEquals("rev_fk_a_1", t5.reverseRuleDependencies().getDeps().get("rev_fk_a_1").getKey().getName());
    assertEquals("rev_fk_a_2", t5.reverseRuleDependencies().getDeps().get("rev_fk_a_2").getKey().getName());
  }
}
