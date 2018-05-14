package com.cosyan.db.transaction;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.MetaRepo;
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

  public abstract void cancel();
}
