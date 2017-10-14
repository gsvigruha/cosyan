package com.cosyan.db;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;

import com.cosyan.db.conf.Config;
import com.cosyan.db.io.IOTestUtil.DummyMaterializedTableMeta;
import com.cosyan.db.io.IOTestUtil.DummyTableReader;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.lock.LockManager;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.sql.Lexer;
import com.cosyan.db.sql.Parser;
import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.SelectStatement.Select;
import com.cosyan.db.sql.SyntaxTree.Statement;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public abstract class DummyTestBase {
  private static MetaRepo metaRepo;
  private static Parser parser;
  private static Lexer lexer;
  private static Resources resources;

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParserException {
    FileUtils.cleanDirectory(new File("/tmp/data"));
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp/data");
    metaRepo = new MetaRepo(new Config(props), new LockManager());
    parser = new Parser();
    lexer = new Lexer();
  }

  public static void register(String name, DummyMaterializedTableMeta table) throws IOException {
    resources = new Resources(
        ImmutableMap.of(name, new DummyTableReader(table.columns(), table.getData())),
        ImmutableMap.of());
    metaRepo.registerTable(name, table);
  }

  protected ExposedTableReader query(String sql) throws ModelException, ParserException, IOException {
    ImmutableList<Statement> tree = parser.parseStatements(lexer.tokenize(sql));
    return ((Select) Iterables.getOnlyElement(tree)).compileTable(metaRepo).reader(resources);
  }
}
