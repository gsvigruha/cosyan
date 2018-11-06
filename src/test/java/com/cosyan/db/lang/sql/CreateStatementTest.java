/*
 * Copyright 2018 Gergely Svigruha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cosyan.db.lang.sql;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.auth.Authenticator.Method;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.meta.Dependencies.TableDependencies;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.session.Session;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.TableMultiIndex;
import com.google.common.collect.ImmutableMap;

public class CreateStatementTest extends UnitTestBase {

  @Test
  public void testCreateTable() throws Exception {
    execute("create table t1 (a varchar not null, b integer, c float, d boolean, e timestamp);");
    MaterializedTable tableMeta = metaRepo.table("admin", "t1");
    assertEquals(new BasicColumn(0, new Ident("a"), DataTypes.StringType, false, false, false),
        tableMeta.column(new Ident("a")));
    assertEquals(new BasicColumn(1, new Ident("b"), DataTypes.LongType, true, false, false),
        tableMeta.column(new Ident("b")));
    assertEquals(new BasicColumn(2, new Ident("c"), DataTypes.DoubleType, true, false, false),
        tableMeta.column(new Ident("c")));
    assertEquals(new BasicColumn(3, new Ident("d"), DataTypes.BoolType, true, false, false),
        tableMeta.column(new Ident("d")));
  }

  @Test
  public void testCreateTableUniqueColumns() throws Exception {
    execute("create table t2 (a varchar unique not null, b integer unique);");
    MaterializedTable tableMeta = metaRepo.table("admin", "t2");
    assertTrue(tableMeta.column(new Ident("a")).isUnique());
    assertTrue(tableMeta.column(new Ident("a")).isIndexed());
    assertFalse(tableMeta.column(new Ident("a")).isNullable());
    assertTrue(tableMeta.column(new Ident("b")).isUnique());
    assertTrue(tableMeta.column(new Ident("b")).isIndexed());
    assertTrue(tableMeta.column(new Ident("b")).isNullable());
  }

  @Test
  public void testCreateTablePrimaryKey() throws Exception {
    execute("create table t3 (a varchar, constraint pk_a primary key (a));");
    MaterializedTable tableMeta = metaRepo.table("admin", "t3");
    assertTrue(tableMeta.column(new Ident("a")).isUnique());
    assertTrue(tableMeta.column(new Ident("a")).isIndexed());
  }

  @Test
  public void testCreateTableForeignKey() throws Exception {
    execute("create table t4 (a varchar, constraint pk_a primary key (a));");
    execute("create table t5 (a varchar, b varchar, constraint fk_b foreign key (b) references t4(a));");
    MaterializedTable t5 = metaRepo.table("admin", "t5");
    assertEquals(new BasicColumn(0, new Ident("a"), DataTypes.StringType, true, false, false),
        t5.column(new Ident("a")));
    assertFalse(t5.column(new Ident("b")).isUnique());
    assertTrue(t5.column(new Ident("b")).isIndexed());
    MaterializedTable t4 = metaRepo.table("admin", "t4");
    assertEquals(ImmutableMap.of("fk_b", new ForeignKey(
        "fk_b",
        "rev_fk_b",
        t5,
        (BasicColumn) t5.column(new Ident("b")),
        t4,
        (BasicColumn) t4.column(new Ident("a")))),
        t5.foreignKeys());
    assertEquals(ImmutableMap.of("rev_fk_b", new ReverseForeignKey(
        "rev_fk_b",
        "fk_b",
        t4,
        (BasicColumn) t4.column(new Ident("a")),
        t5,
        (BasicColumn) t5.column(new Ident("b")))),
        t4.reverseForeignKeys());
  }

  @Test
  public void testCreateTableAlreadyExists() throws Exception {
    execute("create table t6 (a varchar);");
    ErrorResult error = error("create table t6 (a varchar);");
    assertEquals("[13, 15]: Table or view 'admin.t6' already exists.", error.getError().getMessage());
  }

  @Test
  public void testCreateIndex() throws Exception {
    execute("create table t7 (a varchar);");
    execute("create index t7.a;");
    assertEquals(1, metaRepo.table("admin", "t7").multiIndexes().size());
  }

  @Test
  public void testCreateSimpleRule() throws Exception {
    execute("create table t9 (a integer, constraint c_a check (a > 1));");
    Rule rule = metaRepo.table("admin", "t9").rules().get("c_a");
    assertEquals("c_a", rule.getName());
    // TODO test something
  }

  @Test
  public void testCreateReferencingRule() throws Exception {
    execute("create table t10 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t11 (a varchar,"
        + "constraint fk_a foreign key (a) references t10(a),"
        + "constraint c_b check (fk_a.b > 1));");
    MaterializedTable t11 = metaRepo.table("admin", "t11");
    Rule rule = t11.rules().get("c_b");
    assertEquals("c_b", rule.getName());

    assertEquals(1, t11.ruleDependencies().size());
    assertEquals(0, t11.reverseRuleDependencies().getDeps().size());
    assertEquals("fk_a", t11.ruleDependencies().get("fk_a").ref().getName());

    MaterializedTable t10 = metaRepo.table("admin", "t10");
    assertEquals(0, t10.ruleDependencies().size());
    assertEquals(1, t10.reverseRuleDependencies().getDeps().size());
    assertEquals(1, t10.reverseRuleDependencies().getDeps().size());
    assertEquals("rev_fk_a", t10.reverseRuleDependencies().getDeps().get("rev_fk_a").getKey().getName());
    assertEquals(1, t10.reverseRuleDependencies().getDeps().get("rev_fk_a").rules().size());
    assertEquals("c_b",
        t10.reverseRuleDependencies().getDeps().get("rev_fk_a").rule("c_b").getName());
  }

  @Test
  public void testCreateReferencingRuleMultipleLevel() throws Exception {
    execute("create table t12 (a varchar, b integer, constraint pk_a primary key (a));");
    execute("create table t13 (c varchar, d varchar, constraint pk_c primary key (c), "
        + "constraint fk_d foreign key (d) references t12(a));");
    execute("create table t14 (e varchar, constraint fk_e foreign key (e) references t13(c), "
        + "constraint c_b check (fk_e.fk_d.b > 1));");

    MaterializedTable t12 = metaRepo.table("admin", "t12");
    assertEquals(0, t12.ruleDependencies().size());
    assertEquals(1, t12.reverseRuleDependencies().getDeps().size());

    MaterializedTable t13 = metaRepo.table("admin", "t13");
    assertEquals(0, t13.ruleDependencies().size());
    assertEquals(1, t13.reverseRuleDependencies().getDeps().size());

    MaterializedTable t14 = metaRepo.table("admin", "t14");
    assertEquals(1, t14.ruleDependencies().size());
    assertEquals(0, t14.reverseRuleDependencies().getDeps().size());
  }

  @Test
  public void testCreateRefTableTableDeps() throws Exception {
    execute("create table t15 (a varchar, constraint pk_a primary key (a));");
    execute("create table t16 (b varchar, c integer, constraint fk_a foreign key (b) references t15(a));");
    execute("alter table t15 add view s (select sum(c) as sb from rev_fk_a);");

    MaterializedTable t15 = metaRepo.table("admin", "t15");
    TableDependencies deps = t15.meta().table(new Ident("s")).column(new Ident("sb")).tableDependencies();
    assertEquals(1, deps.getDeps().size());
    assertEquals(0, deps.getDeps().get("rev_fk_a").size());
  }

  @Test
  public void testCreateRefTableTableDepsMultipleLevel() throws Exception {
    execute("create table t17 (a varchar, constraint pk_a primary key (a));");
    execute(
        "create table t18 (b varchar, constraint pk_a primary key (b), constraint fk_a foreign key (b) references t17(a));");
    execute("create table t19 (c varchar, d integer, constraint fk_b foreign key (c) references t18(b));");
    execute("alter table t18 add view s (select sum(d) as sd from rev_fk_b);");
    execute("alter table t17 add view s (select sum(s.sd) as ssd from rev_fk_a);");

    MaterializedTable t17 = metaRepo.table("admin", "t17");
    TableDependencies deps = t17.meta().table(new Ident("s")).column(new Ident("ssd")).tableDependencies();
    assertEquals(1, deps.getDeps().size());
    assertEquals(1, deps.getDeps().get("rev_fk_a").size());
    assertEquals(0, deps.getDeps().get("rev_fk_a").dep("rev_fk_b").size());
  }

  @Test
  public void testCreateTableImmutableColumn() throws Exception {
    execute("create table t20 (a varchar immutable);");
    MaterializedTable t20 = metaRepo.table("admin", "t20");
    assertEquals(new BasicColumn(0, new Ident("a"), DataTypes.StringType, true, false, true),
        t20.column(new Ident("a")));
  }

  @Test
  public void testCreateTableIDType() throws Exception {
    execute("create table t21 (a id);");
    MaterializedTable t21 = metaRepo.table("admin", "t21");
    assertTrue(t21.column(new Ident("a")).isUnique());
    assertTrue(t21.column(new Ident("a")).isIndexed());
    assertFalse(t21.column(new Ident("a")).isNullable());
    assertEquals(DataTypes.IDType, t21.column(new Ident("a")).getType());
  }

  @Test
  public void testCreateTableIDTypeErrors() throws Exception {
    ErrorResult e1 = error("create table t22 (a integer, b id);");
    assertEquals("[29, 30]: The ID column 'b' has to be the first one.", e1.getError().getMessage());
    ErrorResult e2 = error("create table t22 (a id, b integer, constraint pk_b primary key (b));");
    assertEquals("[46, 50]: There can only be one primary key.", e2.getError().getMessage());
  }

  @Test
  public void testCreateTableErrorNoReverseForeignKey() throws Exception {
    execute("create table t23 (a id);");
    ErrorResult e1 = error("create table t24 (a integer, "
        + "constraint fk_a foreign key (a) references t23(a), "
        + "constraint c check (b > 1));");
    assertEquals("[100, 101]: Column 'b' not found in table 'admin.t24'.", e1.getError().getMessage());
    MaterializedTable t23 = metaRepo.table("admin", "t23");
    assertEquals(0, t23.reverseForeignKeys().size());
    assertFalse(metaRepo.hasTable("t24", "admin"));
  }

  @Test
  public void testCreateTableCheckNotBoolean() throws Exception {
    ErrorResult e1 = error("create table t25 (a varchar, constraint c check (a.substr(1, 2)));");
    assertEquals("[40, 41]: Constraint check expression has to return a 'boolean': 'a.substr(1, 2)'.",
        e1.getError().getMessage());
  }

  @Test
  public void testCreateIndexWithData() throws Exception {
    execute("create table t26 (a varchar);");
    execute("insert into t26 values ('x'), ('y'), ('x');");
    execute("create index t26.a;");
    MaterializedTable t26 = metaRepo.table("admin", "t26");
    assertTrue(t26.column(new Ident("a")).isIndexed());
    TableMultiIndex index = t26.multiIndexes().get("a");
    assertArrayEquals(new long[] { 0L, 32L }, index.get("x"));
    assertArrayEquals(new long[] { 16L }, index.get("y"));
  }

  @Test
  public void testCreateTableForeignKeyNameResolution() throws Exception {
    execute("create user u1 identified by 'abc';");
    Session u1 = dbApi.authSession("u1", "abc", Method.LOCAL);
    u1.execute("create table t27 (a id);");
    u1.execute("insert into t27 values ('x');");

    ErrorResult e1 = error("create table t28 (a integer, constraint fk_a foreign key (a) references t27);");
    assertError(ModelException.class, "[72, 75]: Table 'admin.t27' does not exist.", e1);

    execute("create table t28 (a integer, constraint fk_a foreign key (a) references u1.t27);");
    MaterializedTable t28 = metaRepo.table("admin", "t28");

    assertFalse(t28.column(new Ident("a")).isUnique());
    assertTrue(t28.column(new Ident("a")).isIndexed());
    MaterializedTable t27 = metaRepo.table("u1", "t27");
    assertEquals(ImmutableMap.of("fk_a", new ForeignKey(
        "fk_a",
        "rev_fk_a",
        t28,
        (BasicColumn) t28.column(new Ident("a")),
        t27,
        (BasicColumn) t27.column(new Ident("a")))),
        t28.foreignKeys());
    assertEquals(ImmutableMap.of("rev_fk_a", new ReverseForeignKey(
        "rev_fk_a",
        "fk_a",
        t27,
        (BasicColumn) t27.column(new Ident("a")),
        t28,
        (BasicColumn) t28.column(new Ident("a")))),
        t27.reverseForeignKeys());
  }

  @Test
  public void testCreateIndexNameResolution() throws Exception {
    execute("create user u2 identified by 'abc';");
    Session u2 = dbApi.authSession("u2", "abc", Method.LOCAL);
    u2.execute("create table t29 (a varchar);");

    ErrorResult e = error("create index t29.a;");
    assertError(ModelException.class, "[13, 16]: Table 'admin.t29' does not exist.", e);

    execute("create index u2.t29.a;");
    assertEquals(1, metaRepo.table("u2", "t29").multiIndexes().size());
  }
}
