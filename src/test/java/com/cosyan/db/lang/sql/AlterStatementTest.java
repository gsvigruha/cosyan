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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.QueryResult;
import com.cosyan.db.meta.MaterializedTable;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Keys.ReverseForeignKey;
import com.cosyan.db.model.Rule;
import com.cosyan.db.model.TableMultiIndex;

public class AlterStatementTest extends UnitTestBase {

  @Test
  public void testDropColumn() throws Exception {
    execute("create table t1 (a varchar, b integer, c float);");
    execute("alter table t1 drop b;");
    MaterializedTable tableMeta = metaRepo.table("admin", "t1");
    assertEquals(2, tableMeta.columns().size());
    assertEquals(new BasicColumn(0, new Ident("a"), DataTypes.StringType, true, false, false),
        tableMeta.column(new Ident("a")));
    assertEquals(new BasicColumn(2, new Ident("c"), DataTypes.DoubleType, true, false, false),
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
    execute("create table t3 (a integer, b integer, constraint pk_b primary key (b), constraint c_a check(a > 1));");
    {
      ErrorResult result = error("alter table t3 drop a;");
      assertEquals("[20, 21]: Cannot drop column 'a', check 'c_a [(a > 1)]' fails.\n" +
          "[1, 2]: Column 'a' not found in table 'admin.t3'.", result.getError().getMessage());
    }
    assertEquals(false, metaRepo.table("admin", "t3").column(new Ident("a")).isDeleted());
    execute("create table t4 (a integer, constraint fk_a foreign key (a) references t3(b));");
    {
      ErrorResult result = error("alter table t4 drop a;");
      assertEquals("[20, 21]: Cannot drop column 'a', it is used by foreign key 'fk_a [a -> admin.t3.b]'.",
          result.getError().getMessage());
    }
    assertEquals(false, metaRepo.table("admin", "t4").column(new Ident("a")).isDeleted());
    {
      ErrorResult result = error("alter table t3 drop b;");
      assertEquals("[20, 21]: Cannot drop column 'b', it is used by reverse foreign key 'rev_fk_a [admin.t4.a -> b]'.",
          result.getError().getMessage());
    }
    assertEquals(false, metaRepo.table("admin", "t3").column(new Ident("b")).isDeleted());
  }

  @Test
  public void testAddColumn() throws Exception {
    execute("create table t5 (a varchar, b integer);");
    execute("alter table t5 add c float;");
    MaterializedTable tableMeta = metaRepo.table("admin", "t5");
    assertEquals(3, tableMeta.columns().size());
    assertEquals(new BasicColumn(0, new Ident("a"), DataTypes.StringType, true, false, false),
        tableMeta.column(new Ident("a")));
    assertEquals(new BasicColumn(1, new Ident("b"), DataTypes.LongType, true, false, false),
        tableMeta.column(new Ident("b")));
    assertEquals(new BasicColumn(2, new Ident("c"), DataTypes.DoubleType, true, false, false),
        tableMeta.column(new Ident("c")));
  }

  @Test
  public void testAddColumnTestData() throws Exception {
    execute("create table t6 (a varchar, b integer);");
    execute("insert into t6 values('x', 1);");
    execute("insert into t6 values('y', 2);");
    execute("alter table t6 add c float;");
    {
      QueryResult result = query("select * from t6;");
      assertHeader(new String[] { "a", "b", "c" }, result);
      assertValues(new Object[][] {
          { "x", 1L, null },
          { "y", 2L, null } }, result);
    }

    execute("insert into t6 values('z', 3, 3.0);");
    {
      QueryResult result = query("select * from t6;");
      assertHeader(new String[] { "a", "b", "c" }, result);
      assertValues(new Object[][] {
          { "x", 1L, null },
          { "y", 2L, null },
          { "z", 3L, 3.0 } }, result);
    }
  }

  @Test
  public void testAddColumnErrors() throws Exception {
    execute("create table t7 (a varchar);");
    {
      ErrorResult result = error("alter table t7 add a varchar;");
      assertEquals("[19, 20]: Duplicate column, foreign key, reversed foreign key or ref name in 't7': 'a'.",
          result.getError().getMessage());
    }
    assertEquals(1, metaRepo.table("admin", "t7").columns().size());
    execute("insert into t7 values ('x');");
    {
      ErrorResult result = error("alter table t7 add b varchar not null;");
      assertEquals("[19, 20]: Cannot add column 'b', new columns on a non empty table have to be nullable.",
          result.getError().getMessage());
    }
    assertEquals(1, metaRepo.table("admin", "t7").columns().size());
  }

  @Test
  public void testDropThenAddColumnWithSameName() throws Exception {
    execute("create table t8 (a varchar, b integer);");
    execute("insert into t8 values('x', 1);");
    execute("alter table t8 drop b;");
    execute("insert into t8 values('y');");
    execute("alter table t8 add b float;");
    execute("insert into t8 values('z', 1.0);");
    QueryResult result = query("select * from t8;");
    assertHeader(new String[] { "a", "b" }, result);
    assertValues(new Object[][] {
        { "x", null },
        { "y", null },
        { "z", 1.0 } }, result);
  }

  @Test
  public void testAddThenDropColumn() throws Exception {
    execute("create table t9 (a varchar, b integer);");
    execute("insert into t9 values('x', 1);");
    execute("alter table t9 add c float;");
    execute("insert into t9 values('y', 2, 2.0);");
    execute("alter table t9 drop b;");
    execute("insert into t9 values('z', 3.0);");
    QueryResult result = query("select * from t9;");
    assertHeader(new String[] { "a", "c" }, result);
    assertValues(new Object[][] {
        { "x", null },
        { "y", 2.0 },
        { "z", 3.0 } }, result);
  }

  @Test
  public void testAlterColumnErrors() throws Exception {
    execute("create table t10 (a varchar, b varchar, c varchar);");
    {
      ErrorResult result = error("alter table t10 alter d varchar;");
      assertEquals("[22, 23]: Column 'd' not found in table 'admin.t10'.", result.getError().getMessage());
    }
    {
      ErrorResult result = error("alter table t10 alter a integer;");
      assertEquals("[22, 23]: Cannot alter column 'a', type has to remain the same.", result.getError().getMessage());
    }
    execute("insert into t10 values ('x', 'y', 'z');");
    {
      ErrorResult result = error("alter table t10 alter b varchar unique;");
      assertEquals("[22, 23]: Cannot alter column 'b', uniqueness has to remain the same.",
          result.getError().getMessage());
    }
    {
      ErrorResult result = error("alter table t10 alter c varchar not null;");
      assertEquals("[22, 23]: Cannot alter column 'c', column has to remain nullable.", result.getError().getMessage());
    }
  }

  @Test
  public void testAlterColumnLiftConstraint() throws Exception {
    execute("create table t11 (a varchar, b float not null);");
    execute("insert into t11 values('x', 1.0);");
    execute("insert into t11 values('y', 2.0);");
    execute("alter table t11 alter b float;");
    execute("insert into t11 (a) values('z');");

    QueryResult result = query("select * from t11;");
    assertHeader(new String[] { "a", "b" }, result);
    assertValues(new Object[][] {
        { "x", 1.0 },
        { "y", 2.0 },
        { "z", null } }, result);
  }

  @Test
  public void testQueryDroppedColumn() throws Exception {
    execute("create table t12 (a varchar, b integer);");
    execute("alter table t12 drop b;");
    ErrorResult result = error("select a, b from t12;");
    assertEquals("[10, 11]: Column 'b' not found in table 'admin.t12'.", result.getError().getMessage());
  }

  @Test
  public void testAlterTableAddConstraintRule() throws Exception {
    execute("create table t13 (a integer);");
    execute("alter table t13 add constraint c1 check (a > 0);");
    ErrorResult result = error("insert into t13 values (0);");
    assertEquals("Constraint check c1 failed.", result.getError().getMessage());
  }

  @Test
  public void testAlterTableAddRef() throws Exception {
    execute("create table t14 (a id, b varchar);");
    execute("create table t15 (a integer, constraint fk_a foreign key (a) references t14);");
    execute("insert into t14 values ('x');");
    execute("insert into t15 values (0);");
    execute("alter table t14 add view s (select count(1) as c from rev_fk_a);");
    QueryResult result = query("select a, b, s.c from t14;");
    assertHeader(new String[] { "a", "b", "c" }, result);
    assertValues(new Object[][] { { 0L, "x", 1L } }, result);
  }

  @Test
  public void testAlterTableAddForeignKeyWithData() throws Exception {
    execute("create table t16 (a id, b varchar);");
    execute("create table t17 (a integer);");
    execute("insert into t16 values ('x'), ('y');");
    execute("insert into t17 values (1), (0), (1);");
    execute("alter table t17 add constraint fk_a foreign key (a) references t16;");

    MaterializedTable t16 = metaRepo.table("admin", "t16");
    MaterializedTable t17 = metaRepo.table("admin", "t17");

    ForeignKey fk = t17.foreignKey(new Ident("fk_a"));
    assertEquals("fk_a", fk.getName());
    assertEquals("rev_fk_a", fk.getRevName());
    assertSame(t17, fk.getTable());
    assertSame(t16, fk.getRefTable());
    assertEquals("a", fk.getColumn().getName());
    assertEquals("a", fk.getRefColumn().getName());

    ReverseForeignKey rfk = t16.reverseForeignKey(new Ident("rev_fk_a"));
    assertEquals("rev_fk_a", rfk.getName());
    assertSame(t16, rfk.getTable());
    assertSame(t17, rfk.getRefTable());
    assertEquals("a", rfk.getColumn().getName());
    assertEquals("a", rfk.getRefColumn().getName());

    assertTrue(t17.column(new Ident("a")).isIndexed());
    TableMultiIndex index = metaRepo.collectMultiIndexes(t17).get("a");
    assertArrayEquals(new long[] { 18L }, index.get(0L));
    assertArrayEquals(new long[] { 0L, 36L }, index.get(1L));
  }

  @Test
  public void testAlterTableAddConstraintRuleWithData() throws Exception {
    execute("create table t18 (a integer);");
    execute("insert into t18 values (1), (2);");
    execute("alter table t18 add constraint c1 check (a > 0);");

    MaterializedTable t18 = metaRepo.table("admin", "t18");
    assertNotNull(t18.rules().get("c1"));

    ErrorResult e = error("insert into t18 values (0);");
    assertEquals("Constraint check c1 failed.", e.getError().getMessage());
  }

  @Test
  public void testAlterTableAddForeignKeyWithBadData() throws Exception {
    execute("create table t19 (a id, b varchar);");
    execute("create table t20 (a integer);");
    execute("insert into t19 values ('x'), ('y');");
    execute("insert into t20 values (1), (0), (2);");
    ErrorResult e = error("alter table t20 add constraint fk_a foreign key (a) references t19;");
    assertEquals("Invalid key '2' (value of 'admin.t20.a'), not found in referenced table 'admin.t19.a'.",
        e.getError().getMessage());

    MaterializedTable t20 = metaRepo.table("admin", "t20");
    assertEquals(0, t20.foreignKeys().size());
    MaterializedTable t19 = metaRepo.table("admin", "t19");
    assertEquals(0, t19.reverseForeignKeys().size());
    assertFalse(t20.column(new Ident("a")).isIndexed());
    assertEquals(0, metaRepo.collectIndexReaders(t20).size());
  }

  @Test
  public void testAlterTableAddConstraintRuleWithBadData() throws Exception {
    execute("create table t21 (a integer);");
    execute("insert into t21 values (1), (2), (0);");
    ErrorResult e = error("alter table t21 add constraint c1 check (a > 0);");
    assertEquals("Constraint check c1 failed.", e.getError().getMessage());
    statement("insert into t21 values (0);");
  }

  @Test
  public void testAlterTableAddConstraintRefRuleWithData() throws Exception {
    execute("create table t22 (a integer, constraint pk_a primary key (a));");
    execute("create table t23 (a integer, b integer, constraint fk_a foreign key (a) references t22);");
    execute("alter table t22 add view s (select sum(b) as b from rev_fk_a);");

    execute("insert into t22 values (1);");
    execute("insert into t23 values (1, 2), (1, 2);");
    execute("alter table t22 add constraint c1 check (s.b < 5);");

    MaterializedTable t22 = metaRepo.table("admin", "t22");
    Rule r1 = t22.rules().get("c1");
    assertNotNull(r1);
    MaterializedTable t23 = metaRepo.table("admin", "t23");
    Rule r2 = t23.reverseRuleDependencies().getDeps().get("fk_a").rule("c1");
    assertNotNull(r2);
    assertSame(r1, r2);

    ErrorResult e = error("insert into t23 values (1, 1);");
    assertEquals("Referencing constraint check t22.c1 failed.", e.getError().getMessage());
  }

  @Test
  public void testAlterTableAddConstraintRefRuleWithBadData() throws Exception {
    execute("create table t24 (a integer, constraint pk_a primary key (a));");
    execute("create table t25 (a integer, b integer, constraint fk_a foreign key (a) references t24);");
    execute("alter table t24 add view s (select sum(b) as b from rev_fk_a);");

    execute("insert into t24 values (1);");
    execute("insert into t25 values (1, 2), (1, 2), (1, 2);");
    ErrorResult e = error("alter table t24 add constraint c1 check (s.b < 5);");
    assertEquals("Constraint check c1 failed.", e.getError().getMessage());

    MaterializedTable t24 = metaRepo.table("admin", "t24");
    assertEquals(0, t24.rules().size());
    MaterializedTable t25 = metaRepo.table("admin", "t25");
    assertEquals(0, t25.reverseRuleDependencies().getDeps().size());

    statement("insert into t25 values (1, 1);");
  }

  @Test
  public void testAlterTableAddForeignKeyWithNullData() throws Exception {
    execute("create table t26 (a id, b varchar);");
    execute("create table t27 (a integer, b varchar);");
    execute("insert into t26 values ('x'), ('y');");
    execute("insert into t27 values (1, 'a'), (0, 'b'), (null, 'c');");
    execute("alter table t27 add constraint fk_a foreign key (a) references t26;");

    MaterializedTable t26 = metaRepo.table("admin", "t26");
    MaterializedTable t27 = metaRepo.table("admin", "t27");

    ForeignKey fk = t27.foreignKey(new Ident("fk_a"));
    assertEquals("fk_a", fk.getName());
    assertEquals("rev_fk_a", fk.getRevName());
    assertSame(t27, fk.getTable());
    assertSame(t26, fk.getRefTable());
    assertEquals("a", fk.getColumn().getName());
    assertEquals("a", fk.getRefColumn().getName());

    ReverseForeignKey rfk = t26.reverseForeignKey(new Ident("rev_fk_a"));
    assertEquals("rev_fk_a", rfk.getName());
    assertSame(t26, rfk.getTable());
    assertSame(t27, rfk.getRefTable());
    assertEquals("a", rfk.getColumn().getName());
    assertEquals("a", rfk.getRefColumn().getName());

    assertTrue(t27.column(new Ident("a")).isIndexed());
    TableMultiIndex index = metaRepo.collectMultiIndexes(t27).get("a");
    assertArrayEquals(new long[] { 25L }, index.get(0L));
    assertArrayEquals(new long[] { 0L }, index.get(1L));
  }

  @Test
  public void testAlterTableDropRule() throws Exception {
    execute("create table t28 (a id, b varchar);");
    execute("create table t29 (a integer, constraint fk_a foreign key (a) references t28);");
    execute("alter table t29 add constraint c_1 check (fk_a.b = 'x');");

    MaterializedTable t28 = metaRepo.table("admin", "t28");
    MaterializedTable t29 = metaRepo.table("admin", "t29");

    Rule rule = t29.rules().get("c_1");
    assertEquals("c_1", rule.getName());
    assertSame(rule, t28.reverseRuleDependencies().getDeps().get("rev_fk_a").rule("c_1"));

    execute("alter table t29 drop constraint c_1;");
    assertFalse(t29.hasRule("c_1"));
    assertNull(t28.reverseRuleDependencies().getDeps().get("rev_fk_a").rule("c_1"));
  }

  @Test
  public void testAlterTableDropForeignKey() throws Exception {
    execute("create table t30 (a id, b varchar);");
    execute("create table t31 (a integer, constraint fk_a foreign key (a) references t30);");

    MaterializedTable t30 = metaRepo.table("admin", "t30");
    MaterializedTable t31 = metaRepo.table("admin", "t31");

    assertEquals("fk_a", t31.foreignKeys().get("fk_a").getName());
    assertEquals("rev_fk_a", t30.reverseForeignKeys().get("rev_fk_a").getName());

    execute("alter table t31 drop constraint fk_a;");
    assertNull(t31.foreignKeys().get("fk_a"));
    assertNull(t30.reverseForeignKeys().get("rev_fk_a"));
  }

  @Test
  public void testAlterTableDropForeignKeyErrors() throws Exception {
    execute("create table t32 (a id, b varchar);");
    execute("create table t33 (a integer, constraint fk_a foreign key (a) references t32);");
    execute("alter table t33 add constraint c check(length(fk_a.b) < 5);");

    MaterializedTable t32 = metaRepo.table("admin", "t32");
    MaterializedTable t33 = metaRepo.table("admin", "t33");

    assertEquals("fk_a", t33.foreignKeys().get("fk_a").getName());
    assertEquals("rev_fk_a", t32.reverseForeignKeys().get("rev_fk_a").getName());

    {
      ErrorResult result = error("alter table t33 drop constraint fk_a;");
      assertEquals(
          "[32, 36]: Cannot drop foreign key 'fk_a', check 'c [(length(fk_a.b) < 5)]' fails.\n[8, 12]: Column 'fk_a' not found in table 'admin.t33'.",
          result.getError().getMessage());
      assertNotNull(metaRepo.table("admin", "t32").reverseForeignKeys().get("rev_fk_a"));
      assertNotNull(metaRepo.table("admin", "t33").foreignKeys().get("fk_a"));
    }

    execute("alter table t32 add view s (select count(1) as cnt from rev_fk_a);");
    {
      ErrorResult result = error("alter table t33 drop constraint fk_a;");
      assertEquals(
          "[32, 36]: Cannot drop foreign key 'fk_a', aggref 's [select count(1) as cnt from rev_fk_a ]' fails.\n[28, 36]: Table 'rev_fk_a' not found.",
          result.getError().getMessage());
      assertNotNull(metaRepo.table("admin", "t32").reverseForeignKeys().get("rev_fk_a"));
      assertNotNull(metaRepo.table("admin", "t33").foreignKeys().get("fk_a"));
    }
  }

  @Test
  public void testAlterTableDropAggRef() throws Exception {
    execute("create table t34 (a id, b varchar);");
    execute("create table t35 (a integer, constraint fk_a foreign key (a) references t34);");
    execute("alter table t34 add view s (select count(1) as cnt from rev_fk_a);");
    execute("alter table t34 add constraint c check(s.cnt < 5);");

    MaterializedTable t34 = metaRepo.table("admin", "t34");

    assertEquals("s", t34.refs().get("s").getName());

    ErrorResult result = error("alter table t34 drop view s;");
    assertEquals(
        "[26, 27]: Cannot drop ref 's', check 'c [(s.cnt < 5)]' fails.\n[1, 2]: Column 's' not found in table 'admin.t34'.",
        result.getError().getMessage());
    assertNotNull(metaRepo.table("admin", "t34").refs().get("s"));

    execute("alter table t34 drop constraint c;");
    execute("alter table t34 drop view s;");
    assertNull(metaRepo.table("admin", "t34").refs().get("s"));
  }

  @Test
  public void testAlterTableAggRefThroughFK() throws Exception {
    execute("create table t36 (c integer, a integer, constraint pk_a primary key (a));");
    execute("create table t37 (b integer, a integer, constraint fk1 foreign key (a) references t36);");
    execute("create table t38 (a integer, constraint fk2 foreign key (a) references t36);");
    execute("alter table t38 add view s (select sum(b) as s from fk2.rev_fk1);");

    execute("insert into t36 values (null, 1), (null, 2), (null, 3);");
    execute("insert into t37 values (2, 1), (2, 1), (5, 2);");
    execute("insert into t38 values (1), (2), (3);");

    {
      QueryResult result = query("select a, s.s from t38;");
      assertHeader(new String[] { "a", "s" }, result);
      assertValues(new Object[][] { { 1L, 4L }, { 2L, 5L }, { 3L, null } }, result);
    }
  }

  @Test
  public void testAlterTableAggRefBackRef() throws Exception {
    execute("create table t39 (a integer, b integer, constraint pk_a primary key (a));");
    execute("create table t40 (a integer, b integer, constraint fk_a foreign key (a) references t39);");
    execute("alter table t39 add view s (select sum(b) as s from rev_fk_a where fk_a.b = b);");

    execute("insert into t39 values (1, 1), (2, 2);");
    execute("insert into t40 values (1, 1), (1, 2), (2, 1), (2, 3);");

    {
      QueryResult result = query("select a, s.s from t39;");
      assertHeader(new String[] { "a", "s" }, result);
      assertValues(new Object[][] { { 1L, 1L }, { 2L, null } }, result);
    }
  }

  @Test
  public void testAlterTableAggRefInAggRef() throws Exception {
    execute("create table t41 (a integer, constraint pk_a primary key (a));");
    execute("create table t42 (a integer, b integer, constraint fk1 foreign key (a) references t41);");
    execute("create table t43 (a integer, constraint fk2 foreign key (a) references t41);");
    execute("alter table t41 add view s (select sum(b) as s from rev_fk1);");
    execute("alter table t43 add view s (select sum(fk1.s.s) as s from fk2.rev_fk1);");

    execute("insert into t41 values (1), (2), (3);");
    execute("insert into t42 values (1, 2), (1, 2), (2, 5);");
    execute("insert into t43 values (1), (2), (3);");

    {
      QueryResult result = query("select a, s.s, fk2.s.s as s2 from t43;");
      assertHeader(new String[] { "a", "s", "s2" }, result);
      assertValues(new Object[][] { { 1L, 8L, 4L }, { 2L, 5L, 5L }, { 3L, null, null } }, result);
    }
  }

  @Test
  public void testAlterTableAggRefParent() throws Exception {
    execute("create table t44 (a varchar, d integer, constraint pk_a primary key (a));");
    execute("create table t45 (a varchar, b integer, constraint fk1 foreign key (a) references t44);");
    execute("create table t46 (a varchar, c integer, constraint fk2 foreign key (a) references t44);");
    execute("alter table t46 add view s (select sum(b * parent.c) as s from fk2.rev_fk1);");

    execute("insert into t44 values ('x', 100), ('y', 200), ('z', 300);");
    execute("insert into t45 values ('x', 2), ('x', 2), ('y', 5);");
    execute("insert into t46 values ('x', 10), ('y', 20), ('z', 30);");

    {
      QueryResult result = query("select a, s.s from t46;");
      assertHeader(new String[] { "a", "s" }, result);
      assertValues(new Object[][] { { "x", 40L }, { "y", 100L }, { "z", null } }, result);
    }

    execute("alter table t44 add view s (select sum(b) as s from rev_fk1 where parent.d = 100);");
    {
      QueryResult result = query("select a, s.s from t44;");
      assertHeader(new String[] { "a", "s" }, result);
      assertValues(new Object[][] { { "x", 4L }, { "y", null }, { "z", null } }, result);
    }
  }

  @Test
  public void testAlterTableAggRefParentError() throws Exception {
    execute("create table t47 (a integer, constraint pk_a primary key (a));");
    execute("create table t48 (a integer, b integer, constraint fk1 foreign key (a) references t47);");

    ErrorResult error = error("select a, parent.a from t48;");
    assertError(ModelException.class, "[10, 16]: Column 'parent' not found in table 'admin.t48'.", error);
  }

  @Test
  public void testAlterTableAggRefAggrColumnName() throws Exception {
    execute("create table t49 (a integer, constraint pk_a primary key (a));");
    execute("create table t50 (a integer, b integer, constraint fk1 foreign key (a) references t49);");
    execute("alter table t49 add view s (select sum(b) as sum from rev_fk1);");

    execute("insert into t49 values (1);");
    execute("insert into t50 values (1, 2), (1, 2);");

    {
      QueryResult result = query("select a, s.sum from t49;");
      assertHeader(new String[] { "a", "sum" }, result);
      assertValues(new Object[][] { { 1L, 4L } }, result);
    }
    {
      QueryResult result = query("select *, s.sum from t49;");
      assertHeader(new String[] { "a", "sum" }, result);
      assertValues(new Object[][] { { 1L, 4L } }, result);
    }
    {
      QueryResult result = query("select a, s.* from t49;");
      assertHeader(new String[] { "a", "sum" }, result);
      assertValues(new Object[][] { { 1L, 4L } }, result);
    }
  }

  @Test
  public void testAlterTableAggRefParentOnSameTable() throws Exception {
    execute("create table t51 (a varchar, constraint pk_a primary key (a));");
    execute("create table t52 (b integer, a varchar, constraint fk1 foreign key (a) references t51);");
    execute("alter table t52 add view s (select count(1) as c from fk1.rev_fk1 where b < parent.b);");

    execute("insert into t51 values ('x'), ('y');");
    execute("insert into t52 values (1, 'x'), (2, 'x'), (3, 'x'), (1, 'y'), (2, 'y');");

    QueryResult result = query("select a, b, s.c from t52;");
    assertHeader(new String[] { "a", "b", "c" }, result);
    assertValues(new Object[][] { { "x", 1L, 0L }, { "x", 2L, 1L }, { "x", 3L, 2L },
        { "y", 1L, 0L }, { "y", 2L, 1L } }, result);
  }

  @Test
  public void testAlterTableAggRefThroughFKInRule() throws Exception {
    execute("create table t53 (a integer, constraint pk_a primary key (a));");
    execute("create table t54 (b integer, a integer, constraint fk1 foreign key (a) references t53);");
    execute("create table t55 (c integer, a integer, constraint fk2 foreign key (a) references t53);");
    execute("alter table t55 add view s (select sum(b) as s from fk2.rev_fk1);");
    execute("alter table t55 add constraint c_1 check(s.s < c);");

    execute("insert into t53 values (1);");
    execute("insert into t55 values (5, 1);");
    execute("insert into t54 values (2, 1), (2, 1);");

    ErrorResult e = error("insert into t54 values (1, 1);");
    assertError(RuleException.class, "Referencing constraint check t55.c_1 failed.", e);
  }

  @Test
  public void testAlterTableAddFlatRef() throws Exception {
    execute("create table t56 (a id, b integer);");
    execute("create table t57 (a integer, constraint fk_a foreign key (a) references t56);");
    execute("insert into t56 values (10);");
    execute("insert into t57 values (0);");
    execute("alter table t57 add view r (select fk_a.b + 1 as c from t57);");
    QueryResult result = query("select r.c from t57;");
    assertHeader(new String[] { "c" }, result);
    assertValues(new Object[][] { { 11L } }, result);
  }

  @Test
  public void testAlterTableFlatRefWithAggRef() throws Exception {
    execute("create table t58 (a id, b integer);");
    execute("create table t59 (a integer, constraint fk_a foreign key (a) references t58);");
    execute("alter table t59 add view f1 (select fk_a.b + 1 as x from t59);");
    execute("alter table t58 add view s (select sum(f1.x) as sx from rev_fk_a);");

    execute("insert into t58 values (10);");
    execute("insert into t59 values (0), (0);");

    {
      QueryResult result = query("select a, s.sx from t58;");
      assertHeader(new String[] { "a", "sx" }, result);
      assertValues(new Object[][] { { 0L, 22L } }, result);
    }
    {
      QueryResult result = query("select a, f1.* from t59;");
      assertHeader(new String[] { "a", "x" }, result);
      assertValues(new Object[][] { { 0L, 11L }, { 0L, 11L } }, result);
    }
  }
}
