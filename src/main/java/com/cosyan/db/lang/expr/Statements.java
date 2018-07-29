package com.cosyan.db.lang.expr;

import java.io.IOException;

import com.cosyan.db.auth.AuthToken;
import com.cosyan.db.lang.transaction.Result;
import com.cosyan.db.meta.Grants.GrantException;
import com.cosyan.db.meta.MetaReader;
import com.cosyan.db.meta.MetaRepo;
import com.cosyan.db.meta.MetaRepo.ModelException;
import com.cosyan.db.meta.MetaRepo.RuleException;
import com.cosyan.db.meta.MetaRepoExecutor;
import com.cosyan.db.transaction.MetaResources;
import com.cosyan.db.transaction.Resources;

public class Statements {
  public static abstract class Statement {

    public abstract MetaResources compile(MetaReader metaRepo) throws ModelException;

    public abstract Result execute(Resources resources) throws RuleException, IOException;

    public abstract void cancel();
  }

  public static abstract class MetaStatement {

  }

  public static abstract class AlterStatement extends MetaStatement {

    public abstract MetaResources executeMeta(MetaRepo metaRepo, AuthToken authToken)
        throws ModelException, GrantException, IOException;

    public abstract Result executeData(MetaRepoExecutor metaRepo, Resources resources)
        throws RuleException, IOException;

    public abstract void cancel();
  }

  public static abstract class GlobalStatement extends MetaStatement {

    public abstract Result execute(MetaRepo metaRepo, AuthToken authToken)
        throws ModelException, GrantException, IOException;
  }
}
