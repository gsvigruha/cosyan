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
import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.model.Ident;
import com.cosyan.db.model.MaterializedTableMeta;

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
    dbApi.getSession().execute("create table t1("
        + "a integer,"
        + "constraint pk_a primary key (a),"
        + "constraint c_a check(a > 1));");
    MaterializedTableMeta tableMeta = dbApi.getMetaRepo().table(new Ident("t1"));
    assertEquals(1, tableMeta.columns().size());
    assertEquals(1, tableMeta.rules().size());
    assertEquals(true, tableMeta.primaryKey().isPresent());
    dbApi = new DBApi(config);
    MaterializedTableMeta newTableMeta = dbApi.getMetaRepo().table(new Ident("t1"));
    assertEquals(tableMeta.columns(), newTableMeta.columns());
    assertEquals(tableMeta.rules(), newTableMeta.rules());
    assertEquals(tableMeta.primaryKey(), newTableMeta.primaryKey());
    assertEquals(tableMeta.foreignKeys(), newTableMeta.foreignKeys());
    assertEquals(tableMeta.reverseForeignKeys(), newTableMeta.reverseForeignKeys());
  }

  @Test
  public void testTableReferencesAfterRestart() throws IOException, ModelException, ParserException {
    DBApi dbApi = new DBApi(config);
    dbApi.getSession().execute("create table t2("
        + "a integer,"
        + "constraint pk_a primary key (a));");
    dbApi.getSession().execute("create table t3("
        + "a integer,"
        + "constraint pk_a primary key (a),"
        + "constraint fk_a1 foreign key (a) references t2(a));");
    dbApi.getSession().execute("create table t4("
        + "a integer,"
        + "constraint fk_a2 foreign key (a) references t2(a));");
    MaterializedTableMeta t2 = dbApi.getMetaRepo().table(new Ident("t2"));
    assertEquals(2, t2.reverseForeignKeys().size());
    MaterializedTableMeta t3 = dbApi.getMetaRepo().table(new Ident("t3"));
    assertEquals(1, t3.foreignKeys().size());
    MaterializedTableMeta t4 = dbApi.getMetaRepo().table(new Ident("t4"));
    assertEquals(1, t4.foreignKeys().size());
    dbApi = new DBApi(config);
    MaterializedTableMeta newT2 = dbApi.getMetaRepo().table(new Ident("t2"));
    MaterializedTableMeta newT3 = dbApi.getMetaRepo().table(new Ident("t3"));
    MaterializedTableMeta newT4 = dbApi.getMetaRepo().table(new Ident("t4"));
    // Foreign keys doesn't have proper equals and hashcode to avoid infinite loops.
    assertEquals(t2.reverseForeignKeys().toString(), newT2.reverseForeignKeys().toString());
    assertEquals(t3.foreignKeys().toString(), newT3.foreignKeys().toString());
    assertEquals(t4.foreignKeys().toString(), newT4.foreignKeys().toString());
  }
}
