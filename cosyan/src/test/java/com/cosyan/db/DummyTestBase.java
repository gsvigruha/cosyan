package com.cosyan.db;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;

import com.cosyan.db.conf.Config;
import com.cosyan.db.io.IOTestUtil.DummyMaterializedTableMeta;
import com.cosyan.db.io.IOTestUtil.DummyTableReader;
import com.cosyan.db.io.TableReader.ExposedTableReader;
import com.cosyan.db.io.TableReader.SeekableTableReader;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.lang.sql.SelectStatement.Select;
import com.cosyan.db.lang.sql.SyntaxTree.Statement;
import com.cosyan.db.lock.LockManager;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.TableMeta.ExposedTableMeta;
import com.cosyan.db.transaction.Resources;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public abstract class DummyTestBase {
  protected static MetaRepo metaRepo;
  private static Parser parser;
  private static Lexer lexer;
  private static Map<String, SeekableTableReader> readers;
  private static Resources resources;

  @BeforeClass
  public static void setUp() throws Exception {
    FileUtils.cleanDirectory(new File("/tmp/data"));
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp/data");
    metaRepo = new MetaRepo(new Config(props), new LockManager());
    parser = new Parser();
    lexer = new Lexer();
    readers = new HashMap<>();
  }

  public static void register(DummyMaterializedTableMeta table) throws IOException {
    readers.put(table.tableName(), new DummyTableReader(table.columns(), table.getData()));
    metaRepo.registerTable(table.tableName(), table);
  }

  public static void finalizeResources() {
    resources = new Resources(ImmutableMap.copyOf(readers), ImmutableMap.of());
  }

  protected ExposedTableReader query(String sql) throws ModelException, ParserException, IOException {
    ImmutableList<Statement> tree = parser.parseStatements(lexer.tokenize(sql));
    ExposedTableMeta tableMeta = ((Select) Iterables.getOnlyElement(tree)).compileTable(metaRepo);
    return new ExposedTableReader(tableMeta, tableMeta.reader(resources));
  }
}
