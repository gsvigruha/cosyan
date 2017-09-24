package com.cosyan.db;

import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.session.Session;
import com.cosyan.db.transaction.TransactionHandler;

import lombok.Data;

@Data
public class DBApi {

  private final MetaRepo metaRepo;
  private final TransactionHandler transactionHandler;
  private final TransactionJournal transactionJournal;

  public Session getSession() {
    return new Session(metaRepo, transactionHandler, transactionJournal);
  }
}
