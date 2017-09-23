package com.cosyan.db;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;

import com.cosyan.db.conf.Config;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.session.Session;
import com.cosyan.db.sql.Result.ErrorResult;
import com.cosyan.db.sql.Result.QueryResult;
import com.cosyan.db.sql.Result.StatementResult;
import com.cosyan.db.sql.Result.TransactionResult;
import com.cosyan.db.transaction.TransactionHandler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public abstract class UnitTestBase {
  private static DBApi dbApi;
  private static Session session;

  protected static MetaRepo metaRepo;

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParseException {
    FileUtils.cleanDirectory(new File("/tmp/data"));
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp/data");
    Config config = new Config(props);
    metaRepo = new MetaRepo(config);
    TransactionHandler transactionHandler = new TransactionHandler();
    TransactionJournal transactionJournal = new TransactionJournal(config);
    dbApi = new DBApi(metaRepo, transactionHandler, transactionJournal);
    session = dbApi.getSession();
  }

  protected void execute(String sql) {
    session.execute(sql);
  }

  protected StatementResult statement(String sql) {
    return (StatementResult) Iterables.getOnlyElement(((TransactionResult) session.execute(sql)).getResults());
  }

  protected ErrorResult error(String sql) {
    return (ErrorResult) session.execute(sql);
  }

  protected QueryResult query(String sql) {
    return (QueryResult) Iterables.getOnlyElement(((TransactionResult) session.execute(sql)).getResults());
  }

  protected void assertHeader(String[] expected, QueryResult result) {
    assertEquals(ImmutableList.copyOf(expected), result.getHeader());
  }

  protected void assertValues(Object[][] expected, QueryResult result) {
    assertEquals(expected.length, result.getValues().size());
    for (int i = 0; i < expected.length; i++) {
      assertEquals(ImmutableList.copyOf(expected[i]), result.getValues().get(i));
    }
  }

  protected void assertError(Class<? extends Exception> clss, String message, ErrorResult result) {
    assertEquals(clss, result.getError().getClass());
    assertEquals(message, result.getError().getMessage());
  }
}
