package com.cosyan.db.meta;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cosyan.db.DBApi;
import com.cosyan.db.conf.Config;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.MaterializedTableMeta;
import com.cosyan.db.sql.Parser.ParserException;
import com.cosyan.db.sql.SyntaxTree.Ident;

public class MetaRepoTest {

  private static Config config;

  @BeforeClass
  public static void before() throws IOException {
    FileUtils.cleanDirectory(new File("/tmp/data"));
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp/data");
    config = new Config(props);
  }

  @Test
  public void testTablesAfterRestart() throws IOException, ModelException, ParserException {
    DBApi dbApi = new DBApi(config);
    dbApi.getSession().execute("create table t1(a integer, constraint c_a check(a > 1));");
    MaterializedTableMeta tableMeta = dbApi.getMetaRepo().table(new Ident("t1"));
    dbApi = new DBApi(config);
    MaterializedTableMeta newTableMeta = dbApi.getMetaRepo().table(new Ident("t1"));
    assertEquals(tableMeta.columns(), newTableMeta.columns());
    assertEquals(tableMeta.simpleCheckDefinitions(), newTableMeta.simpleCheckDefinitions());
    assertEquals(tableMeta.primaryKey(), newTableMeta.primaryKey());
    assertEquals(tableMeta.foreignKeys(), newTableMeta.foreignKeys());
    assertEquals(tableMeta.reverseForeignKeys(), newTableMeta.reverseForeignKeys());
  }
}
