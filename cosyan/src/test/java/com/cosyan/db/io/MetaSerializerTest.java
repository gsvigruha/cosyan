package com.cosyan.db.io;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.cosyan.db.UnitTestBase;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.BasicColumn;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.Keys.ForeignKey;
import com.cosyan.db.model.Rule.BooleanRule;
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
    ForeignKey fk = metaRepo.table("t2").foreignKey(new Ident("fk_a"));
    assertEquals("{\"rev_name\":\"rev_fk_a\",\"name\":\"fk_a\",\"column\":\"b\",\"ref_table\":\"t1\"}",
        serializer.toJSON(fk).toString());
  }

  @Test
  public void testRef() throws ModelException {
    execute("create table t3 (a varchar, constraint pk_a primary key (a));");
    execute("create table t4 (b varchar, c integer, constraint fk_a foreign key (b) references t3);");
    execute("alter table t3 add aggref s (select sum(c) from rev_fk_a);");
    TableRef ref = metaRepo.table("t3").refs().get("s");
    assertEquals("{\"name\":\"s\",\"expr\":\"select sum(c) from rev_fk_a ;\"}",
        serializer.toJSON(ref).toString());
  }

  @Test
  public void testRule() throws ModelException {
    execute("create table t5 (a integer, constraint c_1 check (a > 0));");
    BooleanRule rule = metaRepo.table("t5").rules().get("c_1");
    assertEquals("{\"name\":\"c_1\",\"null_is_true\":true,\"expr\":\"(a > 0)\"}",
        serializer.toJSON(rule).toString());
  }
}
