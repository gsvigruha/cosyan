package com.cosyan.db.sql;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.conf.Config;
import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.model.DataTypes;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.sql.SyntaxTree.Ident;
import com.google.common.collect.ImmutableMap;

public class InsertIntoTest {
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
    Files.deleteIfExists(Paths.get("/tmp/data/t1"));
    Files.deleteIfExists(Paths.get("/tmp/data/t2"));
    Files.deleteIfExists(Paths.get("/tmp/data/t3"));
    Files.deleteIfExists(Paths.get("/tmp/data/t4"));
    Files.deleteIfExists(Paths.get("/tmp/data/t5"));
    Files.deleteIfExists(Paths.get("/tmp/data/t6"));
    Files.createDirectories(Paths.get("/tmp/data"));
  }

  @Test
  public void testInsertIntoTable() throws Exception {
    SyntaxTree create = parser.parse("create table t1 (a varchar, b integer, c float);");
    compiler.statement(create);
    SyntaxTree insert = parser.parse("insert into t1 values ('x', 1, 2.0);");
    compiler.statement(insert);
    SyntaxTree select = parser.parse("select * from t1;");
    ExposedTableMeta tableMeta = compiler.query(select);
    ExposedTableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("a", "x", "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test
  public void testInsertNullsIntoTable() throws Exception {
    compiler.statement(parser.parse("create table t2 (a varchar, b integer, c float);"));
    compiler.statement(parser.parse("insert into t2 values ('x', 1, 2.0);"));
    compiler.statement(parser.parse("insert into t2 (a, c) values ('y', 3.0);"));
    compiler.statement(parser.parse("insert into t2 (c, b) values (4.0, 5);"));
    SyntaxTree select = parser.parse("select * from t2;");
    ExposedTableMeta tableMeta = compiler.query(select);
    ExposedTableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("a", "x", "b", 1L, "c", 2.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", "y", "b", DataTypes.NULL, "c", 3.0), reader.readColumns());
    assertEquals(ImmutableMap.of("a", DataTypes.NULL, "b", 5L, "c", 4.0), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test(expected = ModelException.class)
  public void testNotNullable() throws Exception {
    compiler.statement(parser.parse("create table t3 (a varchar not null, b integer);"));
    compiler.statement(parser.parse("insert into t3 values ('x', 1);"));
    compiler.statement(parser.parse("insert into t3 (b) values (2);"));
  }

  @Test(expected = IndexException.class)
  public void testUniqueNotNull() throws Exception {
    compiler.statement(parser.parse("create table t4 (a varchar unique not null);"));
    compiler.statement(parser.parse("insert into t4 values ('x');"));
    compiler.statement(parser.parse("insert into t4 values ('x');"));
  }

  @Test
  public void testUniqueNull() throws Exception {
    compiler.statement(parser.parse("create table t5 (a varchar unique, b integer);"));
    compiler.statement(parser.parse("insert into t5 values ('x', 1);"));
    compiler.statement(parser.parse("insert into t5 (b) values (1);"));
    compiler.statement(parser.parse("insert into t5 (b) values (1);"));
    SyntaxTree select = parser.parse("select * from t5;");
    ExposedTableMeta tableMeta = compiler.query(select);
    ExposedTableReader reader = tableMeta.reader();
    assertEquals(ImmutableMap.of("a", "x", "b", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", DataTypes.NULL, "b", 1L), reader.readColumns());
    assertEquals(ImmutableMap.of("a", DataTypes.NULL, "b", 1L), reader.readColumns());
    assertEquals(null, reader.readColumns());
  }

  @Test(expected = ModelException.class)
  public void testConstraint() throws Exception {
    compiler.statement(parser.parse("create table t6 (a integer, b integer, constraint c check(a + b > 1));"));
    compiler.statement(parser.parse("insert into t6 values (1, 1);"));
    compiler.statement(parser.parse("insert into t6 values (0, 0);"));
  }
}
