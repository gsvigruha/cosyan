package com.cosyan.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;

import com.cosyan.db.conf.Config;
import com.cosyan.db.lang.sql.Result;
import com.cosyan.db.lang.sql.Parser.ParserException;
import com.cosyan.db.lang.sql.Result.CrashResult;
import com.cosyan.db.lang.sql.Result.ErrorResult;
import com.cosyan.db.lang.sql.Result.QueryResult;
import com.cosyan.db.lang.sql.Result.StatementResult;
import com.cosyan.db.lang.sql.Result.TransactionResult;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public abstract class UnitTestBase {
  private static Session session;

  protected static DBApi dbApi;
  protected static MetaRepo metaRepo;

  @BeforeClass
  public static void setUp() throws IOException, ModelException, ParserException {
    FileUtils.cleanDirectory(new File("/tmp/data"));
    Properties props = new Properties();
    props.setProperty(Config.DATA_DIR, "/tmp/data");
    Config config = new Config(props);
    dbApi = new DBApi(config);
    metaRepo = dbApi.getMetaRepo();
    session = dbApi.getSession();
  }

  protected void execute(String sql) {
    Result result = session.execute(sql);
    if (result instanceof ErrorResult) {
      ((ErrorResult) result).getError().printStackTrace();
      fail();
    }
    assert (!(result instanceof CrashResult));
  }

  protected StatementResult statement(String sql) {
    return (StatementResult) Iterables.getOnlyElement(((TransactionResult) session.execute(sql)).getResults());
  }

  protected ErrorResult error(String sql) {
    return (ErrorResult) session.execute(sql);
  }

  public static QueryResult query(String sql, Session session) {
    Result result = session.execute(sql);
    if (result instanceof TransactionResult) {
      return (QueryResult) Iterables.getOnlyElement(((TransactionResult) result).getResults());
    } else {
      CrashResult crash = (CrashResult) result;
      crash.getError().printStackTrace();
      throw new RuntimeException(crash.getError());
    }
  }

  public static TransactionResult transaction(String sql) {
    return (TransactionResult) session.execute(sql);
  }

  protected QueryResult query(String sql) {
    return query(sql, session);
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
