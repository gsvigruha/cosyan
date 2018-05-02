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
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.session.Session;
import com.cosyan.db.transaction.TransactionHandler;

public class DBApi {

  private final MetaRepo metaRepo;
  private final TransactionHandler transactionHandler;
  private final TransactionJournal transactionJournal;
  private final MetaJournal metaJournal;
  private final Authenticator authenticator;

  public DBApi(Config config) throws IOException, DBException {
    System.out.println("Server starting in root directory " + config.confDir());
    LockManager lockManager = new LockManager();
    authenticator = new Authenticator(config);
    metaRepo = new MetaRepo(config, lockManager, authenticator.localUsers());
    transactionHandler = new TransactionHandler();
    transactionJournal = new TransactionJournal(config);
    metaJournal = new MetaJournal(config);
    Session initSession = adminSession(/* innerSession= */true);
    metaJournal.reload(initSession);
    metaRepo.init();
    System.out.println("Server started.");
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

  public void shutdown() throws IOException {
    metaRepo.shutdown();
  }
}
