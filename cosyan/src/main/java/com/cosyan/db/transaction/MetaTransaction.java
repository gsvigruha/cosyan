package com.cosyan.db.transaction;

import java.io.IOException;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.lang.expr.SyntaxTree.MetaStatement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.ErrorResult;
import com.cosyan.db.lang.transaction.Result.MetaStatementResult;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;

public class MetaTransaction {
  
  private final long trxNumber;
  private final MetaStatement metaStatement;

  public MetaTransaction(long trxNumber, MetaStatement metaStatement) {
    this.trxNumber = trxNumber;
    this.metaStatement = metaStatement;
  }

  public long getTrxNumber() {
    return trxNumber;
  }

  public Result execute(MetaRepo metaRepo, TransactionJournal journal) {
    try {
      metaRepo.metaRepoWriteLock();
      metaStatement.execute(metaRepo);
      return new MetaStatementResult();
    } catch (ModelException | IndexException | IOException e) {
      return new ErrorResult(e);
    } finally {
      metaRepo.metaRepoWriteUnlock();
    }
  }
}
