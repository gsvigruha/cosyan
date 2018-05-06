package com.cosyan.db.transaction;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.CrashResult;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.session.Session;
import com.cosyan.db.transaction.MetaResources.TableMetaResource;

public abstract class Transaction {

  private final long trxNumber;

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

  public Result execute(MetaRepo metaRepo, Session session) {
    TransactionJournal journal = session.transactionJournal();
    metaRepo.metaRepoReadLock();
    MetaResources metaResources;
    try {
      metaResources = collectResources(metaRepo, session.authToken());
    } catch (ModelException | GrantException | IOException e) {
      return new ErrorResult(e);
    } finally {
      metaRepo.metaRepoReadUnlock();
    }
    try {
      for (TableMetaResource resource : metaResources.tables()) {
        metaRepo.checkAccess(resource, session.authToken());
      }
    } catch (GrantException e) {
      return new ErrorResult(e);
    }
    try {
      lock(metaResources, metaRepo);
      journal.start(trxNumber);
      Result result;
      Resources resources = metaRepo.resources(metaResources);
      try {
        result = execute(metaRepo, resources);
      } catch (RuleException e) {
        resources.rollback();
        journal.userError(trxNumber);
        return new ErrorResult(e);
      } catch (IOException e) {
        resources.rollback();
        journal.ioReadError(trxNumber);
        return new CrashResult(e);
      }
      try {
        resources.commit();
        journal.success(trxNumber);
        return result;
      } catch (IOException e) {
        // Need to restore db;
        journal.ioWriteError(trxNumber);
        e.printStackTrace();
        return new CrashResult(e);
      }
    } catch (Throwable e) {
      // Unspecified error, need to restore db;
      journal.crash(trxNumber);
      e.printStackTrace();
      return new CrashResult(e);
    } finally {
      metaRepo.unlock(metaResources);
    }
  }
}
