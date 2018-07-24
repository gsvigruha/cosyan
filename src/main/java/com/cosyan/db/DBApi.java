package com.cosyan.db;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.auth.Authenticator;
import com.cosyan.db.auth.Authenticator.AuthException;
import com.cosyan.db.conf.Config;
import com.cosyan.db.entity.EntityHandler;
import com.cosyan.db.lang.sql.Lexer;
import com.cosyan.db.lang.sql.Parser;
import com.cosyan.db.lock.LockManager;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.session.AdminSession;
import com.cosyan.db.session.Session;
import com.cosyan.db.tools.BackupManager;
import com.cosyan.db.transaction.TransactionHandler;

public class DBApi {

  private final Config config;
  private final MetaRepo metaRepo;
  private final TransactionHandler transactionHandler;
  private final TransactionJournal transactionJournal;
  private final Authenticator authenticator;
  private final BackupManager backupManager;
  private final EntityHandler entityHandler;

  public DBApi(Config config) throws IOException, DBException {
    // System.out.println("Server starting in root directory " + config.confDir());
    this.config = config;
    LockManager lockManager = new LockManager();
    authenticator = new Authenticator(config);
    metaRepo = new MetaRepo(config, lockManager, authenticator.localUsers(), new Lexer(), new Parser());
    transactionHandler = new TransactionHandler();
    transactionJournal = new TransactionJournal(config);
    backupManager = new BackupManager(config, metaRepo);
    entityHandler = new EntityHandler(metaRepo, transactionHandler);
    metaRepo.init();
    // System.out.println("Server started.");
  }

  public MetaRepo getMetaRepo() {
    return metaRepo;
  }

  public EntityHandler entityHandler() {
    return entityHandler;
  }

  public Authenticator authenticator() {
    return authenticator;
  }

  public Config config() {
    return config;
  }

  public Session newAdminSession() {
    return newAdminSession(authenticator.token());
  }

  public Session newAdminSession(String token) {
    return new AdminSession(
        metaRepo,
        transactionHandler,
        transactionJournal,
        backupManager,
        AuthToken.adminToken(token),
        new Parser(),
        new Lexer());
  }

  public Session authSession(String username, String password, Authenticator.Method method) throws AuthException {
    return authSession(authenticator.auth(username, password, method));
  }

  public Session authSession(AuthToken authToken) {
    return new Session(
        metaRepo,
        transactionHandler,
        transactionJournal,
        authToken,
        new Parser(),
        new Lexer());
  }

  public void shutdown() throws IOException {
    metaRepo.shutdown();
  }
}
