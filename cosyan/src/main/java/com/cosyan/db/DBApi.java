package com.cosyan.db;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.auth.Authenticator;
import com.cosyan.db.auth.Authenticator.AuthException;
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
  private final Authenticator authenticator;

  public DBApi(Config config) throws IOException, ModelException, ParserException {
    LockManager lockManager = new LockManager();
    metaRepo = new MetaRepo(config, lockManager);
    transactionHandler = new TransactionHandler();
    transactionJournal = new TransactionJournal(config);
    authenticator = new Authenticator(config);
    metaJournal = new MetaJournal(config);
    metaJournal.reload(adminSession(/* innerSession= */true));
  }

  public MetaRepo getMetaRepo() {
    return metaRepo;
  }

  public Session adminSession() {
    return adminSession(/* innerSession= */false);
  }

  private Session adminSession(boolean innerSession) {
    return new Session(
        metaRepo,
        transactionHandler,
        transactionJournal,
        metaJournal,
        AuthToken.ADMIN_AUTH,
        new Parser(),
        new Lexer(),
        innerSession);
  }

  public Session authSession(String username, String password, Authenticator.Method method) throws AuthException {
    return new Session(
        metaRepo,
        transactionHandler,
        transactionJournal,
        metaJournal,
        authenticator.auth(username, password, method),
        new Parser(),
        new Lexer(),
        /* innerSession= */false);
  }

  public void shutdown() {

  }
}
