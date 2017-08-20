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
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
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
    Files.deleteIfExists(Paths.get("/tmp/data/t1"));
    Files.deleteIfExists(Paths.get("/tmp/data/t2"));
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
}
