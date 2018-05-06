package com.cosyan.db.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.Statement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.TransactionResult;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.google.common.collect.ImmutableList;

public class DataTransaction extends Transaction {
  private final ImmutableList<Statement> statements;

  public DataTransaction(long trxNumber, Iterable<Statement> statements) {
    super(trxNumber);
    this.statements = ImmutableList.copyOf(statements);
  }

  public ImmutableList<Statement> getStatements() {
    return statements;
  }

  @Override
  protected MetaResources collectResources(MetaRepo metaRepo, AuthToken authToken) throws ModelException {
    MetaResources metaResources = MetaResources.empty();
    for (Statement statement : statements) {
      metaResources = metaResources.merge(statement.compile(metaRepo));
    }
    return metaResources;
  }

  @Override
  protected Result execute(MetaRepo metaRepo, Resources resources) throws RuleException, IOException {
    List<Result> results = new ArrayList<>();
    for (Statement statement : statements) {
      results.add(statement.execute(resources));
    }
    return new TransactionResult(results);
  }

  public void cancel() {
    cancelled.set(true);
    for (Statement statement : statements) {
      statement.cancel();
    }
  }
}
