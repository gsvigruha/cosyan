package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.conf.Config;
import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.TableIndex;
import com.cosyan.db.model.TableMultiIndex;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableMap;

public class UpdateTest {
  private static MetaRepo metaRepo;
  private static Parser parser;
  private static Compiler compiler;

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParseException {
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp/data");
    metaRepo = new MetaRepo(new Config(props));
    parser = new Parser();
    compiler = new Compiler(metaRepo);
    FileUtils.cleanDirectory(new File("/tmp/data"));
    Files.createDirectories(Paths.get("/tmp/data"));
  }

  @Test
  public void testUpdateAllRecords() throws Exception {
    compiler.statement(parser.parse("create table t1 (a varchar, b integer, c float);"));
    compiler.statement(parser.parse("insert into t1 values ('x', 1, 2.0);"));
    compiler.statement(parser.parse("insert into t1 values ('y', 3, 4.0);"));
    ExposedTableReader reader = compiler.query(parser.parse("select * from t1;")).reader();
    assertEquals(ImmutableMap.of("a", "x", "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "y", "b", 3L, "c", 4.0), reader.readColumns());
    assertEquals(null, reader.readColumns());

    compiler.statement(parser.parse("update t1 set b = b + 10, c = c * 2;"));
    reader = compiler.query(parser.parse("select * from t1;")).reader();
    assertEquals(ImmutableMap.of("a", "x", "b", 11L, "c", 4.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "y", "b", 13L, "c", 8.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testUpdateWithWhere() throws Exception {
    compiler.statement(parser.parse("create table t2 (a varchar, b integer, c float);"));
    compiler.statement(parser.parse("insert into t2 values ('x', 1, 2.0);"));
    compiler.statement(parser.parse("insert into t2 values ('y', 3, 4.0);"));
    ExposedTableReader reader = compiler.query(parser.parse("select * from t2;")).reader();
    assertEquals(ImmutableMap.of("a", "x", "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "y", "b", 3L, "c", 4.0), reader.readColumns());
    assertEquals(null, reader.readColumns());

    compiler.statement(parser.parse("update t2 set a = 'z' where a = 'x';"));
    reader = compiler.query(parser.parse("select * from t2;")).reader();
    assertEquals(ImmutableMap.of("a", "y", "b", 3L, "c", 4.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "z", "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testUpdateWithIndex() throws Exception {
    compiler.statement(parser.parse("create table t3 (a varchar unique not null, b integer);"));
    compiler.statement(parser.parse("insert into t3 values ('x', 1);"));
    compiler.statement(parser.parse("insert into t3 values ('y', 2);"));

    try {
      compiler.statement(parser.parse("update t3 set a = 'y' where a = 'x';"));
      fail();
    } catch (IndexException e) {
    }
    compiler.statement(parser.parse("update t3 set a = 'z' where a = 'x';"));
    ExposedTableReader reader = compiler.query(parser.parse("select * from t3;")).reader();
    assertEquals(ImmutableMap.of("a", "y", "b", 2L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "z", "b", 1L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testUpdateWithForeignKey() throws Exception {
    compiler.statement(parser.parse("create table t4 (a varchar, constraint pk_a primary key (a));"));
    compiler.statement(
        parser.parse("create table t5 (a varchar, b varchar, constraint fk_b foreign key (b) references t4(a));"));
    compiler.statement(parser.parse("insert into t4 values ('x');"));
    compiler.statement(parser.parse("insert into t4 values ('y');"));
    compiler.statement(parser.parse("insert into t5 values ('123', 'x');"));

    try {
      compiler.statement(parser.parse("update t4 set a = 'z' where a = 'x';"));
      fail();
    } catch (ModelException e) {
    }

    try {
      compiler.statement(parser.parse("update t5 set b = 'z' where b = 'x';"));
      fail();
    } catch (ModelException e) {
    }

    compiler.statement(parser.parse("update t5 set b = 'y' where b = 'x';"));
    ExposedTableReader reader = compiler.query(parser.parse("select * from t5;")).reader();
    assertEquals(ImmutableMap.of("a", "123", "b", "y"), reader.readColumns());

    compiler.statement(parser.parse("delete from t4 where a = 'x';"));
  }

  @Test
  public void testUpdateWithForeignKeyIndexes() throws Exception {
    compiler.statement(parser.parse("create table t6 (a varchar, constraint pk_a primary key (a));"));
    compiler.statement(
        parser.parse("create table t7 (a varchar, b varchar, constraint fk_b foreign key (b) references t6(a));"));
    compiler.statement(parser.parse("insert into t6 values ('x');"));
    compiler.statement(parser.parse("insert into t6 values ('y');"));
    compiler.statement(parser.parse("insert into t7 values ('123', 'x');"));

    TableIndex t6a = metaRepo.collectUniqueIndexes(metaRepo.table(new Ident("t6"))).get("a");
    assertEquals(0L, t6a.get("x"));
    assertEquals(8L, t6a.get("y"));
    TableMultiIndex t7b = metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t7"))).get("b");
    org.junit.Assert.assertArrayEquals(new long[] { 0L }, t7b.get("x"));
    assertEquals(false, t7b.contains("y"));

    compiler.statement(parser.parse("update t7 set b = 'y' where b = 'x';"));
    assertEquals(0L, t6a.get("x"));
    assertEquals(8L, t6a.get("y"));
    assertEquals(false, t7b.contains("x"));
    org.junit.Assert.assertArrayEquals(new long[] { 19L }, t7b.get("y"));
  }
}
