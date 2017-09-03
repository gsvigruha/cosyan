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

public class DeleteTest {
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
  public void testDeleteFromTable() throws Exception {
    compiler.statement(parser.parse("create table t1 (a varchar, b integer, c float);"));
    compiler.statement(parser.parse("insert into t1 values ('x', 1, 2.0);"));
    compiler.statement(parser.parse("insert into t1 values ('y', 3, 4.0);"));
    ExposedTableReader reader = compiler.query(parser.parse("select * from t1;")).reader();
    assertEquals(ImmutableMap.of("a", "x", "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "y", "b", 3L, "c", 4.0), reader.readColumns());
    assertEquals(null, reader.readColumns());

    compiler.statement(parser.parse("delete from t1 where a = 'x';"));
    reader = compiler.query(parser.parse("select * from t1;")).reader();
    assertEquals(ImmutableMap.of("a", "y", "b", 3L, "c", 4.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testDeleteWithIndex() throws Exception {
    compiler.statement(parser.parse("create table t2 (a varchar unique not null, b integer);"));
    compiler.statement(parser.parse("insert into t2 values ('x', 1);"));
    compiler.statement(parser.parse("insert into t2 values ('y', 2);"));
    try {
      compiler.statement(parser.parse("insert into t2 values ('x', 3);"));
      fail();
    } catch (IndexException e) {
    }

    compiler.statement(parser.parse("delete from t2 where a = 'x';"));
    compiler.statement(parser.parse("insert into t2 values ('x', 3);"));

    ExposedTableReader reader = compiler.query(parser.parse("select * from t2;")).reader();
    assertEquals(ImmutableMap.of("a", "y", "b", 2L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "x", "b", 3L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testDeleteWithForeignKey() throws Exception {
    compiler.statement(parser.parse("create table t3 (a varchar, constraint pk_a primary key (a));"));
    compiler.statement(
        parser.parse("create table t4 (a varchar, b varchar, constraint fk_b foreign key (b) references t3(a));"));
    compiler.statement(parser.parse("insert into t3 values ('x');"));
    compiler.statement(parser.parse("insert into t3 values ('y');"));
    compiler.statement(parser.parse("insert into t4 values ('123', 'x');"));

    try {
      compiler.statement(parser.parse("delete from t3 where a = 'x';"));
      fail();
    } catch (ModelException e) {
    }

    compiler.statement(parser.parse("delete from t4 where b = 'x';"));
    compiler.statement(parser.parse("delete from t3 where a = 'x';"));

    ExposedTableReader reader = compiler.query(parser.parse("select * from t3;")).reader();
    assertEquals(ImmutableMap.of("a", "y"), reader.readColumns());
  }

  @Test
  public void testDeleteWithForeignKeyIndexes() throws Exception {
    compiler.statement(parser.parse("create table t5 (a varchar, constraint pk_a primary key (a));"));
    compiler.statement(
        parser.parse("create table t6 (a varchar, b varchar, constraint fk_b foreign key (b) references t5(a));"));
    compiler.statement(parser.parse("insert into t5 values ('x');"));
    compiler.statement(parser.parse("insert into t5 values ('y');"));
    compiler.statement(parser.parse("insert into t6 values ('123', 'x');"));

    TableIndex t5a = metaRepo.collectUniqueIndexes(metaRepo.table(new Ident("t5"))).get("a");
    assertEquals(0L, t5a.get("x"));
    assertEquals(8L, t5a.get("y"));
    TableMultiIndex t6b = metaRepo.collectMultiIndexes(metaRepo.table(new Ident("t6"))).get("b");
    org.junit.Assert.assertArrayEquals(new long[] { 0L }, t6b.get("x"));

    compiler.statement(parser.parse("delete from t6 where b = 'x';"));
    compiler.statement(parser.parse("delete from t5 where a = 'x';"));

    assertEquals(false, t5a.contains("x"));
    assertEquals(8L, t5a.get("y"));
    org.junit.Assert.assertArrayEquals(new long[0], t6b.get("x"));
  }
}
