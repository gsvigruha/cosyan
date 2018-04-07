package com.cosyan.db;

import java.io.IOException;

import com.cosyan.db.conf.Config;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lock.LockManager;
import com.cosyan.db.logging.MetaJournal;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.IParser.ParserException;
import com.cosyan.db.session.Session;
import com.cosyan.db.transaction.TransactionHandler;

public class DBApi {

  private final MetaRepo metaRepo;
  private final TransactionHandler transactionHandler;
  private final TransactionJournal transactionJournal;
  private final MetaJournal metaJournal;

  public DBApi(Config config) throws IOException, ModelException, ParserException {
    LockManager lockManager = new LockManager();
    metaRepo = new MetaRepo(config, lockManager);
    transactionHandler = new TransactionHandler();
    transactionJournal = new TransactionJournal(config);
    metaJournal = new MetaJournal(config);
    metaJournal.reload(getSession(/* innerSession= */true));
  }

  public MetaRepo getMetaRepo() {
    return metaRepo;
  }

  public Session getSession() {
    return getSession(/* innerSession= */false);
  }

  public Session getSession(boolean innerSession) {
    return new Session(
        metaRepo,
        transactionHandler,
        transactionJournal,
        metaJournal,
        new Parser(),
        new Lexer(),
        innerSession);
  }

  public void shutdown() {

  }
}
