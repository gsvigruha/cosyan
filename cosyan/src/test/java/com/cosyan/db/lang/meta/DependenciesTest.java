package com.cosyan.db.lang.meta;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.meta.MaterializedTableMeta;
import com.cosyan.db.meta.Dependencies.ReverseRuleDependency;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.model.Ident;
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
    assertEquals("fk_a", t2.ruleDependencies().get("fk_a").ref().getName());

    MaterializedTableMeta t1 = metaRepo.table(new Ident("t1"));
    assertEquals(0, t1.ruleDependencies().size());
    assertEquals(1, t1.reverseRuleDependencies().getDeps().size());
    assertEquals(1, t1.reverseRuleDependencies().getDeps().size());
    assertEquals("rev_fk_a", t1.reverseRuleDependencies().getDeps().get("rev_fk_a").getKey().getName());
    assertEquals(1, t1.reverseRuleDependencies().getDeps().get("rev_fk_a").rules().size());
    assertEquals("c_b", t1.reverseRuleDependencies().getDeps().get("rev_fk_a").rule("c_b").getName());
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
    assertEquals(0, deps.getDeps().get("rev_fk_a").size());

    assertEquals(1, t3.ruleDependencies().size());
    assertEquals(0, t3.reverseRuleDependencies().getDeps().size());
    assertEquals("rev_fk_a", t3.ruleDependencies().get("rev_fk_a").ref().getName());

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
    assertEquals(0, deps.getDeps().get("fk_a_1").size());
    assertEquals(0, deps.getDeps().get("fk_a_2").size());

    assertEquals(2, t6.ruleDependencies().size());
    assertEquals(0, t6.reverseRuleDependencies().getDeps().size());
    assertEquals("fk_a_1", t6.ruleDependencies().get("fk_a_1").ref().getName());
    assertEquals("fk_a_2", t6.ruleDependencies().get("fk_a_2").ref().getName());

    MaterializedTableMeta t5 = metaRepo.table(new Ident("t5"));
    assertEquals(0, t5.ruleDependencies().size());
    assertEquals(2, t5.reverseRuleDependencies().getDeps().size());
    assertEquals("rev_fk_a_1", t5.reverseRuleDependencies().getDeps().get("rev_fk_a_1").getKey().getName());
    assertEquals("rev_fk_a_2", t5.reverseRuleDependencies().getDeps().get("rev_fk_a_2").getKey().getName());
  }

  @Test
  public void testCreateReferencingRule_MultipleLevels() throws Exception {
    execute("create table t7 (a1 varchar, constraint pk_a primary key (a1));");
    execute("create table t8 (a2 varchar, b2 varchar, "
        + "constraint pk_a primary key (a2),"
        + "constraint fk_b2 foreign key (b2) references t7(a1));");
    execute("create table t9 (b3 varchar, constraint fk_b3 foreign key (b3) references t8(a2));");
    execute("alter table t8 add ref s (select count(b3) as s from rev_fk_b3));");
    execute("alter table t7 add ref s (select sum(s.s) as s from rev_fk_b2));");
    execute("alter table t7 add constraint c check (s.s <= 10);");

    MaterializedTableMeta t7 = metaRepo.table(new Ident("t7"));
    assertEquals(1, t7.ruleDependencies().size());
    assertEquals(0, t7.reverseRuleDependencies().getDeps().size());

    MaterializedTableMeta t8 = metaRepo.table(new Ident("t8"));
    assertEquals(0, t8.ruleDependencies().size());
    assertEquals(1, t8.reverseRuleDependencies().getDeps().size());
    assertEquals(1, t8.reverseRuleDependencies().getDeps().get("fk_b2").rules().size());
    assertEquals("c", t8.reverseRuleDependencies().getDeps().get("fk_b2").rule("c").getName());

    MaterializedTableMeta t9 = metaRepo.table(new Ident("t9"));
    assertEquals(0, t9.ruleDependencies().size());
    assertEquals(1, t9.reverseRuleDependencies().getDeps().size());
    ReverseRuleDependency rev = t9.reverseRuleDependencies().getDeps().get("fk_b3");
    assertEquals(0, rev.rules().size());
    assertEquals(1, rev.getDeps().size());
    assertEquals("c", rev.getDeps().get("fk_b2").rule("c").getName());

    execute("alter table t9 add constraint cs check (fk_b3.fk_b2.s.s <= 10);");
    TableDependencies csDeps = t9.rules().get("cs").getDeps();
    assertEquals(0, csDeps.getDeps()
        .get("fk_b3")
        .dep("fk_b2")
        .dep("rev_fk_b2")
        .dep("rev_fk_b3").size());

    assertEquals(0, t8.ruleDependencies().size());
    assertEquals(2, t8.reverseRuleDependencies().getDeps().size());
    assertEquals(1, t8.reverseRuleDependencies().getDeps().get("fk_b2").rules().size());
    assertEquals(1, t8.reverseRuleDependencies().getDeps().get("rev_fk_b3").rules().size());

    assertEquals(1, t7.ruleDependencies().size());
    assertEquals(1, t7.reverseRuleDependencies().getDeps().size());
    assertEquals(0, t7.reverseRuleDependencies().getDeps().get("rev_fk_b2").rules().size());
    assertEquals(1, t7.reverseRuleDependencies().getDeps().get("rev_fk_b2").getDeps().get("rev_fk_b3").rules().size());
  }
}
