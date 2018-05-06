package com.cosyan.db.transaction;

import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.session.Session;

public abstract class MetaTransaction extends Transaction {

  public MetaTransaction(long trxNumber) {
    super(trxNumber);
  }

  public abstract Result innerExecute(MetaRepo metaRepo, Session session);

}
