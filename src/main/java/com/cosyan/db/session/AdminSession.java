package com.cosyan.db.session;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.logging.MetaJournal.DBException;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.tools.BackupManager;
import com.cosyan.db.transaction.TransactionHandler;

public class AdminSession extends Session {

  private final BackupManager backupManager;

  public AdminSession(
      MetaRepo metaRepo,
      TransactionHandler transactionHandler,
      TransactionJournal transactionJournal,
      BackupManager backupManager,
      AuthToken authToken,
      IParser parser,
      ILexer lexer) {
    super(metaRepo, transactionHandler, transactionJournal, authToken, parser, lexer);
    assert authToken.isAdmin();
    this.backupManager = backupManager;
  }

  public void backup(String name) throws DBException {
    try {
      backupManager.backup(name);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }

  public void restore(String name) throws DBException {
    try {
      backupManager.restore(name);
    } catch (IOException e) {
      throw new DBException(e);
    }
  }
}
