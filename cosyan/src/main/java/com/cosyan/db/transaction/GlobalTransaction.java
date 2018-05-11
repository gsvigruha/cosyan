package com.cosyan.db.transaction;

import java.io.IOException;

import com.cosyan.db.lang.expr.SyntaxTree.GlobalStatement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.CrashResult;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.session.Session;

public class GlobalTransaction extends Transaction {

  private final GlobalStatement globalStatement;

  public GlobalTransaction(long trxNumber, GlobalStatement globalStatement) {
    super(trxNumber);
    this.globalStatement = globalStatement;
  }

  @Override
  public Result execute(MetaRepo metaRepo, Session session) {
    TransactionJournal journal = session.transactionJournal();
    metaRepo.metaRepoReadLock();
    try {
      try {
        journal.start(trxNumber);
        globalStatement.execute(metaRepo, session.authToken());
        metaRepo.writeTables();
        return Result.META_OK;
      } catch (ModelException | GrantException e) {
        // Restore metaRepo.
        metaRepo.readTables();
        journal.userError(trxNumber);
        return new ErrorResult(e);
      } catch (IOException e) {
        // Restore metaRepo.
        metaRepo.readTables();
        journal.ioWriteError(trxNumber);
        return new CrashResult(e);
      }
    }  catch (Throwable e) {
      // Unspecified error, need to restore db;
      journal.crash(trxNumber);
      e.printStackTrace();
      return new CrashResult(e);
    } finally {
      metaRepo.metaRepoReadUnlock();
    }
  }
}
