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
package com.cosyan.db.io;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.View.TopLevelView;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Rule.BooleanRule;
import com.cosyan.db.model.Rule.BooleanViewRule;
import com.cosyan.db.model.TableRef;
import com.google.common.collect.ImmutableList;

public class MetaSerializerTest extends UnitTestBase {

  private MetaSerializer serializer = new MetaSerializer(new Lexer(), new Parser());

  @Test
  public void testColumn() throws ModelException {
    BasicColumn column = new BasicColumn(0, new Ident("a"), DataTypes.DoubleType, false, false, false);
    assertEquals(
        "{\"immutable\":false,\"deleted\":false,\"nullable\":false,\"indexed\":false,\"unique\":false,\"name\":\"a\",\"type\":{\"type\":\"float\"}}",
        serializer.toJSON(column).toString());
  }

  @Test
  public void testEnumColumn() throws ModelException {
    BasicColumn column = new BasicColumn(0, new Ident("a"), DataTypes.enumType(ImmutableList.of("x", "y")), false,
        false, false);
    assertEquals(
        "{\"immutable\":false,\"deleted\":false,\"nullable\":false,\"indexed\":false,\"unique\":false,\"name\":\"a\",\"type\":{\"type\":\"enum\",\"values\":[\"x\",\"y\"]}}",
        serializer.toJSON(column).toString());
  }

  @Test
  public void testForeignKey() throws ModelException {
    execute("create table t1 (a varchar, constraint pk_a primary key (a));");
    execute("create table t2 (b varchar, constraint fk_a foreign key (b) references t1);");
    ForeignKey fk = metaRepo.table("admin", "t2").foreignKey(new Ident("fk_a"));
    assertEquals("{\"rev_name\":\"rev_fk_a\",\"ref_table_owner\":\"admin\",\"ref_table_name\":\"t1\",\"name\":\"fk_a\",\"column\":\"b\"}",
        serializer.toJSON(fk).toString());
  }

  @Test
  public void testRef() throws ModelException {
    execute("create table t3 (a varchar, constraint pk_a primary key (a));");
    execute("create table t4 (b varchar, c integer, constraint fk_a foreign key (b) references t3);");
    execute("alter table t3 add view s (select sum(c) from rev_fk_a);");
    TableRef ref = metaRepo.table("admin", "t3").refs().get("s");
    assertEquals("{\"name\":\"s\",\"index\":1,\"expr\":\"select sum(c) from rev_fk_a ;\"}",
        serializer.toJSON(ref).toString());
  }

  @Test
  public void testRule() throws ModelException {
    execute("create table t5 (a integer, constraint c_1 check (a > 0));");
    BooleanRule rule = metaRepo.table("admin", "t5").rules().get("c_1");
    assertEquals("{\"name\":\"c_1\",\"null_is_true\":true,\"expr\":\"(a > 0)\"}",
        serializer.toJSON(rule).toString());
  }

  @Test
  public void testView() throws ModelException {
    execute("create table t6 (a integer);");
    execute("create view v7 as select count(1) as c from t6 group by a;");
    TopLevelView view = metaRepo.view("admin", "v7");
    assertEquals("{\"owner\":\"admin\",\"name\":\"v7\",\"index\":1,\"expr\":\"select count(1) as c from t6 group by a \",\"rules\":[]}",
        serializer.toJSON(view).toString());
  }

  @Test
  public void testViewRule() throws ModelException {
    execute("create table t8 (a integer);");
    execute("create view v9 as select count(1) as c from t8 group by a;");
    execute("alter view v9 add constraint c_1 check(c < 10);");
    BooleanViewRule rule = metaRepo.view("admin", "v9").rules().get("c_1");
    assertEquals("{\"name\":\"c_1\",\"null_is_true\":true,\"expr\":\"(c < 10)\"}",
        serializer.toJSON(rule).toString());
  }
}
