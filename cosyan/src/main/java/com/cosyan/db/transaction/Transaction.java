package com.cosyan.db.transaction;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.session.Session;

public abstract class Transaction {

  protected final long trxNumber;

  protected AtomicBoolean cancelled = new AtomicBoolean(false);

  public Transaction(long trxNumber) {
    this.trxNumber = trxNumber;
  }

  public long getTrxNumber() {
    return trxNumber;
  }

  protected abstract MetaResources collectResources(MetaRepo metaRepo, AuthToken authToken)
      throws ModelException, GrantException, IOException;

  protected abstract Result execute(MetaRepo metaRepo, Resources resources) throws RuleException, IOException;

  public void lock(MetaResources metaResources, MetaRepo metaRepo) {
    boolean locked = false;
    Random random = new Random();
    while (!locked && !cancelled.get()) {
      if (metaRepo.tryLock(metaResources)) {
        locked = true;
      } else {
        try {
          Thread.sleep(random.nextInt(100));
        } catch (InterruptedException e) {
          cancelled.set(true);
        }
      }
    }
  }

  public abstract Result execute(MetaRepo metaRepo, Session session);
}
