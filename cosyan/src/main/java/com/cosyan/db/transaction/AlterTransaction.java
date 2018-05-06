package com.cosyan.db.transaction;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.expr.SyntaxTree.AlterStatement;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.lang.transaction.Result.CrashResult;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.session.Session;

public class AlterTransaction extends MetaTransaction {

  private final AlterStatement alterStatement;

  public AlterTransaction(long trxNumber, AlterStatement alterStatement) {
    super(trxNumber);
    this.alterStatement = alterStatement;
  }

  @Override
  protected MetaResources collectResources(MetaRepo metaRepo, AuthToken authToken)
      throws ModelException, GrantException, IOException {
    return alterStatement.compile(metaRepo, authToken);
  }

  @Override
  protected Result execute(MetaRepo metaRepo, Resources resources) throws RuleException, IOException {
    return alterStatement.execute(metaRepo, resources);
  }

  public Result innerExecute(MetaRepo metaRepo, Session session) {
    try {
      MetaResources metaResources = collectResources(metaRepo, session.authToken());
      Resources resources = metaRepo.resources(metaResources);
      return alterStatement.execute(metaRepo, resources);
    } catch (RuleException | IOException | ModelException | GrantException e) {
      return new CrashResult(e);
    }
  }
}
