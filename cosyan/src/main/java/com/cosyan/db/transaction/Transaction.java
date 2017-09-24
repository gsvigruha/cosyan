package com.cosyan.db.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosyan.db.index.ByteTrie.IndexException;
import com.cosyan.db.lock.ResourceLock;
import com.cosyan.db.logging.TransactionJournal;
import com.cosyan.db.model.MetaRepo;
import com.cosyan.db.model.MetaRepo.ModelException;
import com.cosyan.db.sql.Result;
import com.cosyan.db.sql.Result.CrashResult;
import com.cosyan.db.sql.Result.ErrorResult;
import com.cosyan.db.sql.Result.TransactionResult;
import com.cosyan.db.sql.SyntaxTree.Statement;
import com.google.common.collect.ImmutableList;

public class Transaction {

  private final long trxNumber;
  private final ImmutableList<Statement> statements;

  private AtomicBoolean cancelled = new AtomicBoolean(false);

  public Transaction(long trxNumber, Iterable<Statement> statements) {
    this.trxNumber = trxNumber;
    this.statements = ImmutableList.copyOf(statements);
  }

  public long getTrxNumber() {
    return trxNumber;
  }

  public ImmutableList<Statement> getStatements() {
    return statements;
  }

  public ImmutableList<ResourceLock> collectLocks() {
    ArrayList<ResourceLock> locks = new ArrayList<>();
    for (Statement statement : statements) {
      statement.collectLocks(locks);
    }
    return ImmutableList.copyOf(locks);
  }

  public void lock(ImmutableList<ResourceLock> locks, MetaRepo metaRepo) {
    boolean locked = false;
    Random random = new Random();
    while (!locked && !cancelled.get()) {
      if (metaRepo.tryLock(locks)) {
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

  public Result execute(MetaRepo metaRepo, TransactionJournal journal) {
    ImmutableList<ResourceLock> locks = collectLocks();
    try {
      lock(locks, metaRepo);
      journal.start(trxNumber);
      List<Result> results = new ArrayList<>();
      try {
        for (Statement statement : statements) {
          results.add(statement.execute(metaRepo));
        }
      } catch (ModelException | IndexException e) {
        for (Statement statement : statements) {
          statement.rollback();
        }
        journal.userError(trxNumber);
        return new ErrorResult(e);
      } catch (IOException e) {
        for (Statement statement : statements) {
          statement.rollback();
        }
        journal.ioReadError(trxNumber);
        return new CrashResult(e);
      }
      try {
        for (Statement statement : statements) {
          statement.commit();
        }
        journal.success(trxNumber);
        return new TransactionResult(results);
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
      metaRepo.unlock(locks);
    }
  }

  public void cancel() {
    cancelled.set(true);
    for (Statement statement : statements) {
      statement.cancel();
    }
  }
}
