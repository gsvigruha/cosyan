package com.cosyan.db.lang.sql;

import java.io.IOException;
import java.util.Optional;

import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.WaitResult;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class WaitStatement extends Statement {

  private final long time;
  private final Optional<String> tag;
  
  private boolean cancelled = false;

  @Override
  public MetaResources compile(MetaRepo metaRepo) throws ModelException {
    return MetaResources.empty();
  }

  @Override
  public Result execute(Resources resources) throws RuleException, IOException {
    long startTime = System.currentTimeMillis();
    try {
      synchronized (this) {
        wait(time);
        if (cancelled) {
          throw new RuleException("Wait cancelled.");
        }
      }
    } catch (InterruptedException e) {
      throw new RuleException("Wait interrupted.");
    }
    long endTime = System.currentTimeMillis();
    return new WaitResult(startTime, endTime, tag);
  }

  @Override
  public void cancel() {
    synchronized (this) {
      cancelled = true;
      notify();
    }
  }
}
