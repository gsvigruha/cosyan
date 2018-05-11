package com.cosyan.db.transaction;

import java.io.IOException;

import com.cosyan.db.lang.expr.SyntaxTree.AlterStatement;
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

public class AlterTransaction extends Transaction {

  private final AlterStatement alterStatement;

  public AlterTransaction(long trxNumber, AlterStatement alterStatement) {
    super(trxNumber);
    this.alterStatement = alterStatement;
  }

  @Override
  public Result execute(MetaRepo metaRepo, Session session) {
    TransactionJournal journal = session.transactionJournal();
    metaRepo.metaRepoReadLock();
    MetaResources metaResources;
    try {
      metaResources = alterStatement.compile(metaRepo, session.authToken());
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
        result = alterStatement.execute(metaRepo, resources);
      } catch (RuleException e) {
        resources.rollback();
        metaRepo.readTables();
        journal.userError(trxNumber);
        return new ErrorResult(e);
      } catch (IOException e) {
        resources.rollback();
        metaRepo.readTables();
        journal.ioReadError(trxNumber);
        return new CrashResult(e);
      }
      try {
        resources.commit();
        metaRepo.writeTables();
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
